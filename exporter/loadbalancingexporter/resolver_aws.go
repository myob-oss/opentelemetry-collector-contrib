package loadbalancingexporter

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
)

var _ resolver = (*awsResolver)(nil)

const (
	defaultAwsResInterval = 5 * time.Second
	defaultAwsResTimeout  = time.Second
)

var (
	awsResolverMutator = tag.Upsert(tag.MustNewKey("resolver"), "aws")

	awsResolverSuccessTrueMutators  = []tag.Mutator{awsResolverMutator, successTrueMutator}
	awsResolverSuccessFalseMutators = []tag.Mutator{awsResolverMutator, successFalseMutator}
)

type awsResolver struct {
	logger      *zap.Logger
	serviceName string
	serviceId   string
	interval    time.Duration
	timeout     time.Duration

	sdSvc sdiface

	endpoints         []string
	onChangeCallbacks []func([]string)

	stopCh             chan (struct{})
	updateLock         sync.Mutex
	shutdownWg         sync.WaitGroup
	changeCallbackLock sync.RWMutex
}

type sdiface interface {
	ListInstances(context.Context, *servicediscovery.ListInstancesInput, ...func(*servicediscovery.Options)) (*servicediscovery.ListInstancesOutput, error)
}

type option func(*awsResolver) error

func getDefaultLookup(r *awsResolver) error {
	// lookup service id from service name
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion("ap-southeast-2"))
	if err != nil {
		return fmt.Errorf("unable to load SDK config, %w", err)
	}

	api := servicediscovery.NewFromConfig(cfg)
	r.sdSvc = api

	out, err := api.ListServices(context.Background(), &servicediscovery.ListServicesInput{})
	if err != nil {
		return fmt.Errorf("failed to list services: %w", err)
	}

	for _, s := range out.Services {
		if s.Name != nil && s.Id != nil && strings.EqualFold(*s.Name, r.serviceName) {
			r.serviceId = *s.Id
		}

	}
	if len(r.serviceId) == 0 {
		return fmt.Errorf("failed to get service id for service name %s", r.serviceName)
	}
	return nil
}

func newAWSResolver(logger *zap.Logger, serviceName string, interval, timeout time.Duration, opts ...option) (*awsResolver, error) {
	if len(serviceName) == 0 {
		return nil, fmt.Errorf("no service name specified")
	}

	if interval == 0 {
		interval = defaultAwsResInterval
	}

	if timeout == 0 {
		timeout = defaultAwsResTimeout
	}

	r := &awsResolver{
		logger:      logger,
		serviceName: serviceName,
		interval:    interval,
		timeout:     timeout,
		stopCh:      make(chan struct{}),
	}

	for _, f := range opts {
		if err := f(r); err != nil {
			return nil, err
		}
	}

	if r.sdSvc == nil {
		if err := getDefaultLookup(r); err != nil {
			return nil, err
		}
	}

	return r, nil
}

func (r *awsResolver) start(ctx context.Context) error {
	if _, err := r.resolve(ctx); err != nil {
		r.logger.Warn("failed to resolve", zap.Error(err))
	}

	go r.periodicallyResolve()

	r.logger.Debug("AWS resolver started",
		zap.String("service name", r.serviceName), zap.String("service id", r.serviceId),
		zap.Duration("interval", r.interval), zap.Duration("timeout", r.timeout))
	return nil
}

func (r *awsResolver) resolve(ctx context.Context) ([]string, error) {
	r.shutdownWg.Add(1)
	defer r.shutdownWg.Done()

	var backends []string

	resp, err := r.sdSvc.ListInstances(ctx, &servicediscovery.ListInstancesInput{
		ServiceId: &r.serviceId,
	})
	if err != nil {
		_ = stats.RecordWithTags(ctx, awsResolverSuccessFalseMutators, mNumResolutions.M(1))
		return nil, err
	} else {
		_ = stats.RecordWithTags(ctx, awsResolverSuccessTrueMutators, mNumResolutions.M(1))
		for _, i := range resp.Instances {
			backends = append(backends, i.Attributes["AWS_INSTANCE_IPV4"])
		}
	}

	sort.Strings(backends)

	if equalStringSlice(r.endpoints, backends) {
		return r.endpoints, nil
	}

	// the list has changed!
	r.updateLock.Lock()
	r.endpoints = backends
	r.updateLock.Unlock()
	_ = stats.RecordWithTags(ctx, resolverSuccessTrueMutators, mNumBackends.M(int64(len(backends))))

	r.logger.Warn("AWS resolver changed", zap.Strings("instances", backends))

	// propagate the change
	r.changeCallbackLock.RLock()
	for _, callback := range r.onChangeCallbacks {
		callback(r.endpoints)
	}
	r.changeCallbackLock.RUnlock()

	return r.endpoints, nil
}

func (r *awsResolver) periodicallyResolve() {
	ticker := time.NewTicker(r.interval)

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
			if _, err := r.resolve(ctx); err != nil {
				r.logger.Warn("failed to resolve", zap.Error(err))
			} else {
				r.logger.Debug("resolved successfully")
			}
			cancel()
		case <-r.stopCh:
			return
		}
	}

}

func (r *awsResolver) shutdown(ctx context.Context) error {
	r.changeCallbackLock.Lock()
	r.onChangeCallbacks = nil
	r.changeCallbackLock.Unlock()

	close(r.stopCh)
	r.shutdownWg.Wait()
	return nil
}

func (r *awsResolver) onChange(f func([]string)) {
	r.changeCallbackLock.Lock()
	defer r.changeCallbackLock.Unlock()
	r.onChangeCallbacks = append(r.onChangeCallbacks, f)

}
