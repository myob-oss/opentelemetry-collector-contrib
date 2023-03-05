package loadbalancingexporter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	"github.com/aws/aws-sdk-go-v2/service/servicediscovery/types"
)

type mockAwsServiceDiscovery struct{}

func (m *mockAwsServiceDiscovery) ListInstances(ctx context.Context, params *servicediscovery.ListInstancesInput, optFns ...func(*servicediscovery.Options)) (*servicediscovery.ListInstancesOutput, error) {

	return &servicediscovery.ListInstancesOutput{
		Instances: []types.InstanceSummary{
			{
				Id: aws.String("instance1"),
				Attributes: map[string]string{
					"AWS_INSTANCE_IPV4": "127.0.0.2",
				},
			},
			{
				Id: aws.String("instance2"),
				Attributes: map[string]string{
					"AWS_INSTANCE_IPV4": "127.0.0.1",
				},
			},
		},
	}, nil
}

func withOptionTestLookup(r *awsResolver) error {
	r.serviceId = "test-service-id"
	r.sdSvc = &mockAwsServiceDiscovery{}
	return nil
}

func TestServiceDiscovery(t *testing.T) {
	res, err := newAWSResolver(
		zaptest.NewLogger(t),
		"test-service",
		0,
		0,
		withOptionTestLookup,
	)
	require.NoError(t, err)

	resolved := []string{}

	res.onChange(func(endpoints []string) {
		resolved = endpoints
	})

	require.NoError(t, res.start(context.Background()))
	defer func() {
		require.NoError(t, res.shutdown(context.Background()))
	}()

	assert.Len(t, resolved, 2)
	for i, value := range []string{"127.0.0.1", "127.0.0.2"} {
		assert.Equal(t, value, resolved[i])
	}

}
