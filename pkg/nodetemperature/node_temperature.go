package nodetemperature

import (
	"context"
	"fmt"
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"

	"sigs.k8s.io/scheduler-plugins/pkg/apis/config"
)

var _ = framework.ScorePlugin(&NodeTemperature{})

// Name is the name of the plugin used in the Registry and configurations.
const (
	Name                      = "NodeTemperature"
	AnnotationNodeTemperature = "annotator.enflame.cn/node-temperature"
	AnnotationRackTemperature = "annotator.enflame.cn/rack-temperature"
	AnnotationDtu0Temperature = "annotator.enflame.cn/dtu0-temperature"
	AnnotationDtu1Temperature = "annotator.enflame.cn/dtu1-temperature"
	AnnotationDtu2Temperature = "annotator.enflame.cn/dtu2-temperature"
	AnnotationDtu3Temperature = "annotator.enflame.cn/dtu3-temperature"
	AnnotationDtu4Temperature = "annotator.enflame.cn/dtu4-temperature"
	AnnotationDtu5Temperature = "annotator.enflame.cn/dtu5-temperature"
	AnnotationDtu6Temperature = "annotator.enflame.cn/dtu6-temperature"
	AnnotationDtu7Temperature = "annotator.enflame.cn/dtu7-temperature"
)

type NodeTemperature struct {
	args   *config.NodeTemperatureArgs
	handle framework.Handle
}

// New initializes a new plugin and returns it.
func New(obj runtime.Object, h framework.Handle) (framework.Plugin, error) {
	args, ok := obj.(*config.NodeTemperatureArgs)
	if !ok {
		return nil, fmt.Errorf("[NodeTemperature] want args to be of type NodeTemperatureArgs, got %T", obj)
	}

	klog.Infof("[NodeTemperature] args received: %+v;", *args)
	return &NodeTemperature{args: args, handle: h}, nil
}

func (nt *NodeTemperature) Name() string {
	return Name
}

// Score invoked at the score extension point.
func (nt *NodeTemperature) Score(ctx context.Context,
	state *framework.CycleState,
	pod *v1.Pod,
	nodeName string) (int64, *framework.Status) {
	klog.Info("[NodeTemperature] start scoring...")

	nodeInfo, err := nt.handle.SnapshotSharedLister().NodeInfos().Get(nodeName)
	if err != nil {
		return 0, framework.NewStatus(framework.Error, fmt.Sprintf("getting node %q from Snapshot: %v", nodeName, err))
	}

	var score int64

	annotations := nodeInfo.Node().Annotations
	if annotations != nil && len(annotations) > 0 {
		tempScoreFunc := func(annoName string, weight float64, maxTempValue float64, defaultTempValue float64) float64 {
			annoValue, ok := annotations[annoName]
			tempValue := defaultTempValue
			if ok {
				v, err := strconv.ParseFloat(annoValue, 64)
				if err != nil {
					klog.Warningf("Parsing annotaion %s value: %v", annoName, err)
				} else {
					tempValue = v
				}
			}

			if tempValue > maxTempValue {
				tempValue = maxTempValue
			}
			return tempValue * weight
		}

		var tempTotalScore float64
		tempTotalScore += tempScoreFunc(AnnotationRackTemperature, nt.args.RackTemperatureWeight, nt.args.MaxRackTemperature, nt.args.DefaultRackTemperature)
		tempTotalScore += tempScoreFunc(AnnotationNodeTemperature, nt.args.NodeTemperatureWeight, nt.args.MaxNodeTemperature, nt.args.DefaultNodeTemperature)
		tempTotalScore += tempScoreFunc(AnnotationDtu0Temperature, nt.args.DtuTemperatureWeight, nt.args.MaxDtuTemperature, nt.args.DefaultDtuTemperature)
		tempTotalScore += tempScoreFunc(AnnotationDtu1Temperature, nt.args.DtuTemperatureWeight, nt.args.MaxDtuTemperature, nt.args.DefaultDtuTemperature)
		tempTotalScore += tempScoreFunc(AnnotationDtu2Temperature, nt.args.DtuTemperatureWeight, nt.args.MaxDtuTemperature, nt.args.DefaultDtuTemperature)
		tempTotalScore += tempScoreFunc(AnnotationDtu3Temperature, nt.args.DtuTemperatureWeight, nt.args.MaxDtuTemperature, nt.args.DefaultDtuTemperature)
		tempTotalScore += tempScoreFunc(AnnotationDtu4Temperature, nt.args.DtuTemperatureWeight, nt.args.MaxDtuTemperature, nt.args.DefaultDtuTemperature)
		tempTotalScore += tempScoreFunc(AnnotationDtu5Temperature, nt.args.DtuTemperatureWeight, nt.args.MaxDtuTemperature, nt.args.DefaultDtuTemperature)
		tempTotalScore += tempScoreFunc(AnnotationDtu6Temperature, nt.args.DtuTemperatureWeight, nt.args.MaxDtuTemperature, nt.args.DefaultDtuTemperature)
		tempTotalScore += tempScoreFunc(AnnotationDtu7Temperature, nt.args.DtuTemperatureWeight, nt.args.MaxDtuTemperature, nt.args.DefaultDtuTemperature)

		tempTotalMaxScore := nt.args.MaxRackTemperature + nt.args.MaxNodeTemperature + nt.args.MaxDtuTemperature*8
		score = 100 - int64(tempTotalScore/tempTotalMaxScore)
	}

	klog.V(6).InfoS("[NodeTemperature] score for host", "nodeName", nodeName, "score", score)
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
