package pvsync

import (
	"context"
	"fmt"
	"time"
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"k8s.io/klog/v2"
	"sigs.k8s.io/yaml"

	workv1alpha1 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha1"
	workv1alpha2 "github.com/karmada-io/karmada/pkg/apis/work/v1alpha2"
	"github.com/karmada-io/karmada/pkg/util/helper"
	"github.com/karmada-io/karmada/pkg/util/names"
	"github.com/karmada-io/karmada/pkg/controllers/ctrlutil"
)

const PVMigrationControllerName = "pv-migration-controller"

type PVMigrationController struct {
	client.Client
	EventRecorder record.EventRecorder
	RESTMapper    meta.RESTMapper
}

func (c *PVMigrationController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	klog.Infof("[PVMigrationController] Reconciling ResourceBinding")

	rb := &workv1alpha2.ResourceBinding{}
	if err := c.Client.Get(ctx, req.NamespacedName, rb); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 1. Check target is StatefulSet
	if rb.Spec.Resource.Kind != "StatefulSet" || rb.Spec.Resource.APIVersion != "apps/v1" {
		return ctrl.Result{}, nil
	}

	// 2. Check if any cluster reports "Unhealthy"
	unhealthy := false
	for _, s := range rb.Status.AggregatedStatus {
		if s.Health == "Unhealthy" {
			unhealthy = true
			break
		}
	}
	if !unhealthy {
		return ctrl.Result{}, nil
	}

	// 3. Check if dispatching is suspended
	if rb.Spec.Suspension != nil && rb.Spec.Suspension.Dispatching != nil && *rb.Spec.Suspension.Dispatching {
		klog.Infof("[Matched RB] %s/%s: Unhealthy StatefulSet, dispatching suspended", rb.Namespace, rb.Name)
	}
	
	// 4. Build source StatefulSet key
	stsKey := rb.Spec.Resource.Namespace + "." + rb.Spec.Resource.Name

	// 5. Get PV metadata Works created by PVSyncController
	workList := &workv1alpha1.WorkList{}
	if err := c.Client.List(ctx, workList, client.MatchingLabels{
		"pvsync.karmada.io/type":       "metadata",
		"pvsync.karmada.io/source-sts": stsKey,
	}); err != nil {
		klog.Errorf("Failed to list PV metadata Works for %s: %v", stsKey, err)
		return ctrl.Result{}, err
	}

	// 6. Extract clusters from RB spec
	currentClusterSet := map[string]bool{}
	for _, cluster := range rb.Spec.Clusters {
		currentClusterSet[cluster.Name] = true
	}

	// 7. Extract clusters from metadata Work namespace
	metaWorkClusterSet := map[string]bool{}
	for _, w := range workList.Items {
		clusterName, err := names.GetClusterName(w.Namespace)
		if err != nil {
			continue
		}
		metaWorkClusterSet[clusterName] = true
	}

	// 8. Compare clusters
	if !equalStringSet(currentClusterSet, metaWorkClusterSet) {
		newClusters := difference(currentClusterSet, metaWorkClusterSet)
		klog.Infof("[Scheduling Changed] %s/%s: 스케줄링 결과가 바뀌었고 재스케줄링된 클러스터는 %v", rb.Namespace, rb.Name, newClusters)
		// 9. View PV-metadata work configmap
		for _, cluster := range newClusters {
			for _, w := range workList.Items {
				for _, manifest := range w.Spec.Workload.Manifests {
					var u unstructured.Unstructured
					if err := yaml.Unmarshal(manifest.Raw, &u); err != nil {
						klog.Warningf("Failed to parse manifest as Unstructured: %v", err)
						continue
					}
					if u.GetKind() != "ConfigMap" {
						continue
					}
	
					var cm corev1.ConfigMap
					if err := yaml.Unmarshal(manifest.Raw, &cm); err != nil {
						klog.Warningf("Failed to decode embedded ConfigMap: %v", err)
						continue
					}
					// 10. Create PV work
					for pvName, pvSpecYaml := range cm.Data {
						err := c.createPVWork(ctx, cluster, rb, pvName, pvSpecYaml)
						if err != nil {
							klog.Errorf("Failed to create PV Work for %s in cluster %s: %v", pvName, cluster, err)
						}
					}
				}
			}
		}
	}

	//11. If PV deploy successfully, delete previous metadata work and PV work
	_ = c.cleanupAfterBoundPV(ctx, rb, workList, currentClusterSet)

	return ctrl.Result{}, nil
}
//ms: function for 8
func equalStringSet(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}
//ms: function for 8
func difference(a, b map[string]bool) []string {
	var diff []string
	for k := range a {
		if !b[k] {
			diff = append(diff, k)
		}
	}
	return diff
}
//ms: function for 10
func (c *PVMigrationController) createPVWork(ctx context.Context, clusterName string, rb *workv1alpha2.ResourceBinding, pvName string, pvSpecYaml string) error {
	var fullpvSpec corev1.PersistentVolumeSpec
	if err := yaml.Unmarshal([]byte(pvSpecYaml), &fullpvSpec); err != nil {
		klog.Errorf("Failed to unmarshal PV spec YAML: %v", err)
		return err
	}
	var claimRef *corev1.ObjectReference
	if fullpvSpec.ClaimRef != nil {
		claimRef = &corev1.ObjectReference{
			APIVersion: fullpvSpec.ClaimRef.APIVersion,
			Kind:       fullpvSpec.ClaimRef.Kind,
			Name:       fullpvSpec.ClaimRef.Name,
			Namespace:  fullpvSpec.ClaimRef.Namespace,
		}
	}
	//ms: just copy selected field that we want
	pvSpec := corev1.PersistentVolumeSpec{
		AccessModes:                   fullpvSpec.AccessModes,
		Capacity:                      fullpvSpec.Capacity,
		ClaimRef:                      claimRef,
		PersistentVolumeReclaimPolicy: fullpvSpec.PersistentVolumeReclaimPolicy,
		StorageClassName:              fullpvSpec.StorageClassName,
		VolumeMode:                    fullpvSpec.VolumeMode,
		PersistentVolumeSource:        corev1.PersistentVolumeSource{
        		NFS: fullpvSpec.PersistentVolumeSource.NFS, 
    		},	
	}
	//ms: if pv work existed, skip
	workName := fmt.Sprintf("pv-work-%s-%s", rb.Name, clusterName)
	workNamespace := names.GenerateExecutionSpaceName(clusterName)
	existing := &workv1alpha1.Work{}
	if err := c.Client.Get(ctx, client.ObjectKey{
		Name:      workName,
		Namespace: workNamespace,
	}, existing); err == nil {
		klog.Infof("🔁 PV Work already exists for cluster %s. Skipping creation.", clusterName)
		return nil
	}
	//ms: create pv name
	generatedName := fmt.Sprintf("pv-%s-%s", rb.Name, clusterName)
	var existingPVs corev1.PersistentVolumeList
	if err := c.Client.List(ctx, &existingPVs); err == nil {
		for _, existingPV := range existingPVs.Items {
			if existingPV.Spec.ClaimRef != nil &&
				claimRef != nil &&
				existingPV.Spec.ClaimRef.Name == claimRef.Name &&
				existingPV.Spec.ClaimRef.Namespace == claimRef.Namespace {
					klog.Infof("🔁 PV for PVC %s/%s already exists. Skipping creation.", claimRef.Namespace, claimRef.Name)
					return nil
				}
		}
	}
	newPV := &corev1.PersistentVolume{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PersistentVolume",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: generatedName,
		},
		Spec: pvSpec,
	}
	
	unstructuredPV, err := helper.ToUnstructured(newPV)
	if err != nil {
		klog.Errorf("Failed to convert PV to unstructured: %v", err)
		return err
	}

	workMeta := metav1.ObjectMeta{
		Name:      fmt.Sprintf("pv-work-%s-%s", rb.Name, clusterName),
		Namespace: names.GenerateExecutionSpaceName(clusterName),
		Labels: map[string]string{
			"pvsync.karmada.io/type":       "pv-deployment",
			"pvsync.karmada.io/source-sts": rb.Spec.Resource.Namespace + "." + rb.Spec.Resource.Name,
			"pvsync.karmada.io/source-rb":  rb.Name,
		},
	}

	if err := ctrlutil.CreateOrUpdateWork(ctx, c.Client, workMeta, unstructuredPV); err != nil {
		klog.Errorf("Failed to create PV Work for cluster %s: %v", clusterName, err)
		return err
	}

	klog.Infof("Created PV Work for cluster %s using PV %s", clusterName, newPV.Name)
	return nil
}
func (c *PVMigrationController) cleanupAfterBoundPV(ctx context.Context,rb *workv1alpha2.ResourceBinding,workList *workv1alpha1.WorkList,currentClusterSet map[string]bool,) error {
	klog.Infof("cleanup start")
	pvWorkList := &workv1alpha1.WorkList{}
	if err := c.Client.List(ctx, pvWorkList, client.MatchingLabels{
		"pvsync.karmada.io/type":      "pv-deployment",
		"pvsync.karmada.io/source-rb": rb.Name,
	}); err != nil {
		return fmt.Errorf("failed to list PV Works: %w", err)
	}
	const (
		maxRetries    = 10
		retryInterval = 2 * time.Second
	)	
	for _, pvWork := range pvWorkList.Items {
		klog.Infof("🔍 Checking PV Work: %s/%s", pvWork.Namespace, pvWork.Name)

		isAvailable := false
		for i := 0; i < maxRetries; i++ {
			// 최신 상태 가져오기
			latest := &workv1alpha1.Work{}
			if err := c.Client.Get(ctx, client.ObjectKeyFromObject(&pvWork), latest); err != nil {
				klog.Warningf("Failed to re-fetch PV Work: %v", err)
				break
			}

			for _, m := range latest.Status.ManifestStatuses {
				if m.Identifier.Kind != "PersistentVolume" || m.Health != "Healthy" {
					continue
				}
				var phaseStruct struct {
					Phase string `json:"phase"`
				}
				if err := json.Unmarshal(m.Status.Raw, &phaseStruct); err != nil {
					klog.Warningf("Failed to parse status.phase: %v", err)
					continue
				}
				klog.Infof("Retry %d: phase = %s", i+1, phaseStruct.Phase)

				if phaseStruct.Phase == "Available"|| phaseStruct.Phase == "Bound" {
					isAvailable = true
					break
				}
			}

			if isAvailable {
				break
			}
			time.Sleep(retryInterval)
		}

		if !isAvailable {
			klog.Warningf("⏳ PV Work %s/%s not ready after retries, skipping cleanup", pvWork.Namespace, pvWork.Name)
			continue
		}

		// ✅11.1. suspension delete first
		if rb.Spec.Suspension != nil && rb.Spec.Suspension.Dispatching != nil && *rb.Spec.Suspension.Dispatching {
			freshRB := &workv1alpha2.ResourceBinding{}
			if err := c.Client.Get(ctx, client.ObjectKeyFromObject(rb), freshRB); err != nil {
				klog.Errorf("❌ Failed to get fresh ResourceBinding: %v", err)
				return err
			}
			if freshRB.Spec.Suspension == nil {
				freshRB.Spec.Suspension = &workv1alpha2.Suspension{}
			}
			falseVal := false
			freshRB.Spec.Suspension.Dispatching = &falseVal

			if err := c.Client.Update(ctx, freshRB); err != nil {
				klog.Warningf("❌ Still failed to update suspension.dispatching: %v", err)
				return err
			} else {
				klog.Infof("✅ Successfully updated suspension.dispatching to false")
			}
		}
		// ✅ 11.2. PV Work delete
		if err := c.Client.Delete(ctx, &pvWork); err != nil {
			klog.Warningf("Failed to delete PV Work %s: %v", pvWork.Name, err)
		} else {
			klog.Infof("🧹 Deleted PV Work %s", pvWork.Name)
		}

		// ✅ 11.3. PV metadata Work delete(except now deployed pv metadata work)
		for _, w := range workList.Items {
			metaCluster, err := names.GetClusterName(w.Namespace)
			if err != nil {
				continue
			}
			if !currentClusterSet[metaCluster] {
				if err := c.Client.Delete(ctx, &w); err != nil {
					klog.Warningf("Failed to delete outdated metadata Work from %s: %v", metaCluster, err)
				} else {
					klog.Infof("🧹 Deleted outdated PV metadata Work from %s", metaCluster)
				}
			}
		}
	}
	return nil
}
func (c *PVMigrationController) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		Named(PVMigrationControllerName).
		For(&workv1alpha2.ResourceBinding{}). // no predicate, triggers on create/update
		Complete(c)
}
