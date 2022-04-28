package nodetemperature

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"sigs.k8s.io/scheduler-plugins/pkg/apis/config"
	"sigs.k8s.io/scheduler-plugins/pkg/nodetemperature/metricswatcher"
	metricswatcherapi "sigs.k8s.io/scheduler-plugins/pkg/nodetemperature/metricswatcher/api"
)

var _ = framework.PostFilterPlugin(&NodeTemperature{})
var _ = framework.ScorePlugin(&NodeTemperature{})

// Name is the name of the plugin used in the Registry and configurations.
const (
	// Time interval in seconds for each metrics agent ingestion.
	metricsAgentReportingIntervalSeconds = 60
	metricsUpdateIntervalSeconds         = 30
	Name                                 = "NodeTemperature"
)

type NodeTemperature struct {
	handle  framework.Handle
	client  metricswatcherapi.Client
	metrics metricswatcher.WatcherMetrics
	// For safe access to metrics
	mu sync.RWMutex
}

// New initializes a new plugin and returns it.
func New(obj runtime.Object, h framework.Handle) (framework.Plugin, error) {
	args, ok := obj.(*config.NodeTemperatureArgs)
	if !ok {
		return nil, fmt.Errorf("[NodeTemperature] want args to be of type NodeTemperatureArgs, got %T", obj)
	}

	klog.Infof("[NodeTemperature] args received: %+v;", *args)

	metricProviderType := string(args.MetricProvider.Type)
	validMetricProviderType := metricProviderType == string(config.KubernetesMetricsServer) ||
		metricProviderType == string(config.Prometheus) ||
		metricProviderType == string(config.SignalFx)
	if !validMetricProviderType {
		return nil, fmt.Errorf("invalid MetricProvider.Type, got %T", args.MetricProvider.Type)
	}

	opts := metricswatcher.MetricsProviderOpts{string(args.MetricProvider.Type), args.MetricProvider.Address, args.MetricProvider.Token}
	client, err := metricswatcherapi.NewLibraryClient(opts)
	if err != nil {
		return nil, fmt.Errorf("[NodeTemperature] error creating metrics watcher client: %v", err)
	}

	nt := &NodeTemperature{
		handle: h,
		client: client,
	}

	// populate metrics before returning
	err = nt.updateMetrics()
	if err != nil {
		klog.ErrorS(err, "Unable to populate metrics initially")
	}
	go func() {
		metricsUpdaterTicker := time.NewTicker(time.Second * metricsUpdateIntervalSeconds)
		for range metricsUpdaterTicker.C {
			err = nt.updateMetrics()
			if err != nil {
				klog.ErrorS(err, "Unable to update metrics")
			}
		}
	}()

	return nt, nil
}

func (nt *NodeTemperature) Name() string {
	return Name
}

func (nt *NodeTemperature) PostFilter(ctx context.Context,
	state *framework.CycleState,
	pod *v1.Pod,
	filteredNodeStatusMap framework.NodeToStatusMap) (*framework.PostFilterResult, *framework.Status) {
	return nil, nil
}

// Score invoked at the score extension point.
func (nt *NodeTemperature) Score(ctx context.Context,
	state *framework.CycleState,
	pod *v1.Pod,
	nodeName string) (int64, *framework.Status) {
	//nodeInfo, err := nt.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	//if err != nil {
	//	return 0, framework.NewStatus(framework.Error, fmt.Sprintf("getting node %q from Snapshot: %v", nodeName, err))
	//}

	// copy value lest updateMetrics() updates it and to avoid locking for rest of the function
	nt.mu.RLock()
	metrics := nt.metrics
	nt.mu.RUnlock()

	// This happens if metrics were never populated since scheduler started
	if metrics.Data.NodeMetricsMap == nil {
		klog.ErrorS(nil, "Metrics not available from watcher, assigning 0 score to node", "nodeName", nodeName)
		return framework.MinNodeScore, nil
	}

	// This means the node is new (no metrics yet) or metrics are unavailable due to 404 or 500
	if _, ok := metrics.Data.NodeMetricsMap[nodeName]; !ok {
		klog.InfoS("Unable to find metrics for node", "nodeName", nodeName)
		// Avoid the node by scoring minimum
		return framework.MinNodeScore, nil
	}

	score := int64(99)
	klog.V(6).InfoS("Score for host", "nodeName", nodeName, "score", score)

	return score, framework.NewStatus(framework.Success, "")
}

// ScoreExtensions of the Score plugin.
func (nt *NodeTemperature) ScoreExtensions() framework.ScoreExtensions {
	return nt
}

func (nt *NodeTemperature) NormalizeScore(ctx context.Context,
	state *framework.CycleState,
	pod *v1.Pod,
	scores framework.NodeScoreList) *framework.Status {
	return nil
}

func (nt *NodeTemperature) updateMetrics() error {
	metrics, err := nt.client.GetLatestWatcherMetrics()
	if err != nil {
		return err
	}

	nt.mu.Lock()
	nt.metrics = *metrics
	nt.mu.Unlock()

	return nil
}
