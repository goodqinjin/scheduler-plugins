/*
Copyright 2020 PayPal

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package metricswatcher

import "os"

const (
	K8sClientName      = "KubernetesMetricsServer"
	PromClientName     = "Prometheus"
	SignalFxClientName = "SignalFx"

	MetricsProviderNameKey    = "METRICS_PROVIDER_NAME"
	MetricsProviderAddressKey = "METRICS_PROVIDER_ADDRESS"
	MetricsProviderTokenKey   = "METRICS_PROVIDER_TOKEN"
)

var (
	EnvMetricProviderOpts MetricsProviderOpts
)

func init() {
	var ok bool
	EnvMetricProviderOpts.Name, ok = os.LookupEnv(MetricsProviderNameKey)
	if !ok {
		EnvMetricProviderOpts.Name = K8sClientName
	}
	EnvMetricProviderOpts.Address, ok = os.LookupEnv(MetricsProviderAddressKey)
	EnvMetricProviderOpts.AuthToken, ok = os.LookupEnv(MetricsProviderTokenKey)
}

// Interface to be implemented by any metrics provider client to interact with Watcher
type MetricsProviderClient interface {
	// Return the client name
	Name() string
	// Fetch metrics for given host
	FetchHostMetrics(host string, window *Window) ([]Metric, error)
	// Fetch metrics for all hosts
	FetchAllHostsMetrics(window *Window) (map[string][]Metric, error)
}

// Generic metrics provider options
type MetricsProviderOpts struct {
	Name      string
	Address   string
	AuthToken string
}