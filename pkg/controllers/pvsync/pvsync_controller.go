package pvsync

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"sigs.k8s.io/yaml"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	workv1alpha1 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha1"
	"github.com/karmada-io/karmada/pkg/util"
	"github.com/karmada-io/karmada/pkg/util/fedinformer/genericmanager"
	"github.com/karmada-io/karmada/pkg/util/names"
	"github.com/karmada-io/karmada/pkg/util/restmapper"
)

const ControllerName = "pv-sync-controller"

type PVSyncController struct {
	client.Client
	RESTMapper                  meta.RESTMapper
	InformerManager             genericmanager.MultiClusterInformerManager
	EventRecorder               record.EventRecorder
	ClusterDynamicClientSetFunc func(clusterName string, client client.Client) (*util.DynamicClusterClient, error)
	ClusterCacheSyncTimeout     metav1.Duration
	StopChan      <-chan struct{}
	PredicateFunc predicate.Predicate
}

// Reconcile - list PVs in the member cluster and log them
func (c *PVSyncController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	klog.V(4).Infof("Reconciling Work %s", req.NamespacedName)

	work := &workv1alpha1.Work{}
	if err := c.Client.Get(ctx, req.NamespacedName, work); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	//ms: 5th modify, sts filtering
	var sourceStsName, sourceStsNamespace string
	hasStatefulSet := false

	for _, m := range work.Spec.Workload.Manifests {
		var obj map[string]interface{}
		if err := json.Unmarshal(m.RawExtension.Raw, &obj); err != nil {
			continue
		}
		if kind, ok := obj["kind"]; ok && kind == "StatefulSet" {
			metadata := obj["metadata"].(map[string]interface{})
			sourceStsName = metadata["name"].(string)
			if ns, ok := metadata["namespace"].(string); ok {
				sourceStsNamespace = ns
			} else {
				sourceStsNamespace = "default"
			}
			hasStatefulSet = true
			break
		}
	}
	//ms: 5th modify 
	if !hasStatefulSet {
		klog.V(4).Infof("Skipping Work %s: no StatefulSet found", req.NamespacedName)
		return ctrl.Result{}, nil
	}

	clusterName, err := names.GetClusterName(work.GetNamespace())
	if err != nil {
		return ctrl.Result{}, err
	}

	cluster, err := util.GetCluster(c.Client, clusterName)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !util.IsClusterReady(&cluster.Status) {
		klog.Infof("Cluster %s is not ready. Skip.", clusterName)
		return ctrl.Result{}, nil
	}

	dynamicClient, err := c.ClusterDynamicClientSetFunc(clusterName, c.Client)
	if err != nil {
		klog.Errorf("Failed to create dynamic client for cluster %s: %v", clusterName, err)
		return ctrl.Result{}, err
	}

	// Get GVR for PV
	pvGVR, err := restmapper.GetGroupVersionResource(c.RESTMapper, corev1.SchemeGroupVersion.WithKind("PersistentVolume"))
	if err != nil {
		return ctrl.Result{}, err
	}
	
	pvList, err := dynamicClient.DynamicClientSet.Resource(pvGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Failed to list PVs from cluster %s: %v", clusterName, err)
		return ctrl.Result{}, err
	}
	
	//ms: 2nd modify, Build ConfigMap with PV metadata
	configMap := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pv-metadata-" + clusterName,
			Namespace: "default",
		},
		Data: map[string]string{},
	}	
	klog.Infof("===== PVs from cluster [%s] =====", clusterName)
	//ms: 3rd modify, make pv configmap
	for _, item := range pvList.Items {
		pvName := item.GetName()

		spec, ok := item.Object["spec"]
		if !ok {
			continue
		}

		specBytes, err := yaml.Marshal(spec)
		if err != nil {
			klog.Warningf("Failed to marshal PV spec for %s: %v", pvName, err)
			continue
		}

		configMap.Data[pvName] = string(specBytes)
	}
	//ms: 6th modify 
	if len(configMap.Data) == 0 {
		klog.Infof("No valid PVs found in cluster %s. Skipping PV metadata Work creation.", clusterName)
		return ctrl.Result{}, nil
	}
	//ms: 2nd modify
	rawObj, err := json.Marshal(configMap)
	if err != nil {
		return ctrl.Result{}, err
	}

	manifest := workv1alpha1.Manifest{
		RawExtension: runtime.RawExtension{
			Raw: rawObj,
		},
	}	
	//ms: 2nd modify, Create Work with manifest
	workName := "pv-metadata-" + clusterName
	execNamespace := names.GenerateExecutionSpaceName(clusterName)
	//ms: 5th modify, add label of owner sts
	metaWork := &workv1alpha1.Work{
		ObjectMeta: metav1.ObjectMeta{
			Name:      workName,
			Namespace: execNamespace,
			Labels: map[string]string{
				"pvsync.karmada.io/type":        "metadata",
				"pvsync.karmada.io/source-sts":  fmt.Sprintf("%s.%s", sourceStsNamespace, sourceStsName),
				"pvsync.karmada.io/source-work": work.Name,
			},
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, c.Client, metaWork, func() error {
		//ms: 4th modify, work suspension setting
		suspend := true
		metaWork.Spec.SuspendDispatching = &suspend

		metaWork.Spec.Workload = workv1alpha1.WorkloadTemplate{
			Manifests: []workv1alpha1.Manifest{manifest},
		}
		return nil
	})
	if err != nil {
		klog.Errorf("Failed to create or update PV metadata Work for cluster %s: %v", clusterName, err)
		return ctrl.Result{}, err
	}

	klog.Infof("Created/Updated PV metadata Work for cluster %s", clusterName)
	return ctrl.Result{}, nil
}

// SetupWithManager registers controller to manager
func (c *PVSyncController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(ControllerName).
		For(&workv1alpha1.Work{}, builder.WithPredicates()). // work 기반 트리거
		Complete(c)
}

