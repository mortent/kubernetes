/*
Copyright 2024 The Kubernetes Authors.

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

package dra

import (
	"strings"
	"time"

	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	schedulingv1alpha2 "k8s.io/api/scheduling/v1alpha2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"k8s.io/kubernetes/test/utils/ktesting"
)

func testPreemption(tCtx ktesting.TContext, alphaApisEnabled, betaApisEnabled bool) {
	tCtx.Parallel()

	startScheduler(tCtx)
	startClaimController(tCtx)

	runSubTest(tCtx, "direct-claim-preemption", testDirectClaimPreemption)
	runSubTest(tCtx, "template-preemption", testTemplatePreemption)
	if alphaApisEnabled {
		runSubTest(tCtx, "podgroup-preemption", testPodGroupPreemption)
	}
	if betaApisEnabled {
		runSubTest(tCtx, "consumable-capacity-preemption", testConsumableCapacityPreemption)
		runSubTest(tCtx, "partitionable-devices-preemption", testPartitionableDevicesPreemption)
		runSubTest(tCtx, "binding-conditions-preemption", testBindingConditionsPreemption)
	}
}

// testDirectClaimPreemption verifies that a high-priority pod can preempt a low-priority pod
// when both reference a ResourceClaim directly. It ensures that the victim pod is marked
// for deletion and receives the DisruptionTarget condition.
func testDirectClaimPreemption(tCtx ktesting.TContext) {
	namespace := createTestNamespace(tCtx, nil)

	tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
		events, err := tCtx.Client().CoreV1().Events(namespace).List(tCtx, metav1.ListOptions{})
		if err == nil {
			tCtx.Logf("--- Events for namespace %s ---", namespace)
			for _, e := range events.Items {
				tCtx.Logf("Event %s/%s: %s", e.InvolvedObject.Kind, e.InvolvedObject.Name, e.Message)
			}
		}
	})

	class, driverName := createTestClass(tCtx, namespace)

	// Create a single device slice on worker-0
	slice := st.MakeResourceSlice("worker-0", driverName).Devices("device-1")
	createSlice(tCtx, slice.Obj())

	createPriorityClass(tCtx, "low-priority", 100)
	createPriorityClass(tCtx, "high-priority", 200)

	// A low priority pod with a single claim takes the only device.
	victimClaim := createClaim(tCtx, namespace, "-victim", class, claim)
	victimPod := podWithClaimName.DeepCopy()
	victimPod.Spec.PriorityClassName = "low-priority"
	victim := createPod(tCtx, namespace, "-victim", victimPod, victimClaim)

	// Wait for victim to be scheduled
	victim = waitForPodScheduled(tCtx, namespace, victim.Name)

	// Make sure the reservedFor field has been updated by the scheduler. We check this sepearately
	// from the PodScheduled check since this is a check on the claim rather than the pod.
	tCtx.Eventually(func(tCtx ktesting.TContext) int {
		c, err := tCtx.Client().ResourceV1().ResourceClaims(namespace).Get(tCtx, victimClaim.Name, metav1.GetOptions{})
		if err != nil {
			return 0
		}
		return len(c.Status.ReservedFor)
	}).WithTimeout(30 * time.Second).Should(gomega.Equal(1), "victim claim should be reserved for the pod")

	// A high priority pod references a claim requesting a device from the same class.
	preemptorClaim := createClaim(tCtx, namespace, "-preemptor", class, claim)
	preemptorPod := podWithClaimName.DeepCopy()
	preemptorPod.Spec.PriorityClassName = "high-priority"
	_ = createPod(tCtx, namespace, "-preemptor", preemptorPod, preemptorClaim)

	// wait for victim to be marked for eviction (deletion timestamp and disruption target)
	tCtx.Eventually(func(tCtx ktesting.TContext) bool {
		p, err := tCtx.Client().CoreV1().Pods(namespace).Get(tCtx, victim.Name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		if p.DeletionTimestamp == nil {
			return false
		}
		_, cond := podutil.GetPodCondition(&p.Status, v1.DisruptionTarget)
		return cond != nil && cond.Status == v1.ConditionTrue
	}).WithTimeout(30 * time.Second).Should(gomega.BeTrue(), "Victim pod should be preempted (deletion timestamp set and DisruptionTarget condition true)")
}

// testTemplatePreemption verifies that preemption works when pods use ResourceClaimTemplates.
// It ensures that the generated claims are correctly handled during preemption dry-runs
// and that the victim pod is preempted when a higher priority pod needs the resource.
func testTemplatePreemption(tCtx ktesting.TContext) {
	namespace := createTestNamespace(tCtx, nil)

	tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
		events, err := tCtx.Client().CoreV1().Events(namespace).List(tCtx, metav1.ListOptions{})
		if err == nil {
			tCtx.Logf("--- Events for namespace %s ---", namespace)
			for _, e := range events.Items {
				tCtx.Logf("Event %s/%s: %s", e.InvolvedObject.Kind, e.InvolvedObject.Name, e.Message)
			}
		}
	})

	class, driverName := createTestClass(tCtx, namespace)

	slice := st.MakeResourceSlice("worker-0", driverName).Devices("device-1")
	createSlice(tCtx, slice.Obj())

	createPriorityClass(tCtx, "low-priority", 100)
	createPriorityClass(tCtx, "high-priority", 200)

	claim := st.MakeResourceClaim().Name("my-claim").Namespace(namespace).Request(class.Name).Obj()
	template := &resourceapi.ResourceClaimTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "my-template", Namespace: namespace},
		Spec: resourceapi.ResourceClaimTemplateSpec{
			Spec: claim.Spec,
		},
	}
	_, err := tCtx.Client().ResourceV1().ResourceClaimTemplates(namespace).Create(tCtx, template, metav1.CreateOptions{})
	tCtx.ExpectNoError(err, "create template")

	victimPod := podWithClaimName.DeepCopy()
	victimPod.Spec.PriorityClassName = "low-priority"
	victim := createPod(tCtx, namespace, "-victim-0", victimPod, template)
	victim = waitForPodScheduled(tCtx, namespace, victim.Name)

	// Retrieve the generated claim
	claims, err := tCtx.Client().ResourceV1().ResourceClaims(namespace).List(tCtx, metav1.ListOptions{})
	tCtx.ExpectNoError(err, "list generated claims")
	if len(claims.Items) != 1 {
		tCtx.Fatalf("Expected exactly 1 generated claim, got %d", len(claims.Items))
	}
	generatedClaimName := claims.Items[0].Name

	// Make sure the reservedFor field has been updated by the scheduler.
	tCtx.Eventually(func(tCtx ktesting.TContext) int {
		c, err := tCtx.Client().ResourceV1().ResourceClaims(namespace).Get(tCtx, generatedClaimName, metav1.GetOptions{})
		if err != nil {
			return 0
		}
		return len(c.Status.ReservedFor)
	}).WithTimeout(30 * time.Second).Should(gomega.Equal(1), "generated claim should be reserved for the pod")

	preemptorClaim := createClaim(tCtx, namespace, "-preemptor", class, claim)
	preemptorPod := podWithClaimName.DeepCopy()
	preemptorPod.Spec.PriorityClassName = "high-priority"
	_ = createPod(tCtx, namespace, "-preemptor", preemptorPod, preemptorClaim)

	// wait for victim to be marked for eviction (deletion timestamp and disruption target)
	tCtx.Eventually(func(tCtx ktesting.TContext) bool {
		p, err := tCtx.Client().CoreV1().Pods(namespace).Get(tCtx, victim.Name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		if p.DeletionTimestamp == nil {
			return false
		}
		_, cond := podutil.GetPodCondition(&p.Status, v1.DisruptionTarget)
		return cond != nil && cond.Status == v1.ConditionTrue
	}).WithTimeout(30 * time.Second).Should(gomega.BeTrue(), "Victim pod should be preempted (deletion timestamp set and DisruptionTarget condition true)")
}

// testPodGroupPreemption verifies that the scheduler does NOT preempt pods that belong to a PodGroup
// to avoid breaking gang scheduling guarantees.
// It ensures the preemptor fails to find victims and the victim remains running.
func testPodGroupPreemption(tCtx ktesting.TContext) {
	namespace := createTestNamespace(tCtx, nil)

	tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
		events, err := tCtx.Client().CoreV1().Events(namespace).List(tCtx, metav1.ListOptions{})
		if err == nil {
			tCtx.Logf("--- Events for namespace %s ---", namespace)
			for _, e := range events.Items {
				tCtx.Logf("Event %s/%s: %s", e.InvolvedObject.Kind, e.InvolvedObject.Name, e.Message)
			}
		}
	})
	class, driverName := createTestClass(tCtx, namespace)

	slice := st.MakeResourceSlice("worker-0", driverName).Devices("device-1")
	createSlice(tCtx, slice.Obj())

	createPriorityClass(tCtx, "low-priority", 100)
	createPriorityClass(tCtx, "high-priority", 200)

	victimClaim := createClaim(tCtx, namespace, "-victim", class, claim)

	podGroup := &schedulingv1alpha2.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-pod-group",
			Namespace: namespace,
		},
		Spec: schedulingv1alpha2.PodGroupSpec{
			SchedulingPolicy: schedulingv1alpha2.PodGroupSchedulingPolicy{
				Basic: &schedulingv1alpha2.BasicSchedulingPolicy{},
			},
			ResourceClaims: []schedulingv1alpha2.PodGroupResourceClaim{
				{
					Name:              victimClaim.Name,
					ResourceClaimName: &victimClaim.Name,
				},
			},
		},
	}
	var err error
	podGroup, err = tCtx.Client().SchedulingV1alpha2().PodGroups(namespace).Create(tCtx, podGroup, metav1.CreateOptions{})
	tCtx.ExpectNoError(err, "create PodGroup")

	victimPod := podWithClaimName.DeepCopy()
	victimPod.Spec.PriorityClassName = "low-priority"
	victimPod.Spec.SchedulingGroup = &v1.PodSchedulingGroup{
		PodGroupName: &podGroup.Name,
	}
	victim := createPod(tCtx, namespace, "-victim", victimPod, victimClaim)

	victim = waitForPodScheduled(tCtx, namespace, victim.Name)

	// Make sure the reservedFor field has been updated by the system to point to the PodGroup.
	tCtx.Eventually(func(tCtx ktesting.TContext) (*resourceapi.ResourceClaim, error) {
		return tCtx.Client().ResourceV1().ResourceClaims(namespace).Get(tCtx, victimClaim.Name, metav1.GetOptions{})
	}).WithTimeout(30 * time.Second).Should(gomega.HaveField("Status.ReservedFor", gomega.ConsistOf(gomega.HaveField("UID", gomega.Equal(podGroup.UID)))), "victim claim should be reserved strictly for the podgroup")

	// A high priority pod wants same class
	preemptorClaim := createClaim(tCtx, namespace, "-preemptor", class, claim)
	preemptorPod := podWithClaimName.DeepCopy()
	preemptorPod.Spec.PriorityClassName = "high-priority"
	preemptor := createPod(tCtx, namespace, "-preemptor", preemptorPod, preemptorClaim)

	// wait for preemptor to officially fail to find preemption victims on any node
	tCtx.Eventually(func(tCtx ktesting.TContext) bool {
		p, err := tCtx.Client().CoreV1().Pods(namespace).Get(tCtx, preemptor.Name, metav1.GetOptions{})
		if err != nil {
			return false
		}
		for _, cond := range p.Status.Conditions {
			if cond.Type == v1.PodScheduled && cond.Status == v1.ConditionFalse && cond.Reason == "Unschedulable" && strings.Contains(cond.Message, "No preemption victims found") {
				return true
			}
		}
		return false
	}).WithTimeout(10 * time.Second).Should(gomega.BeTrue(), "Preemptor should be unschedulable")

	// Ensure the victim pod was NOT marked for eviction
	pod, _ := tCtx.Client().CoreV1().Pods(namespace).Get(tCtx, victim.Name, metav1.GetOptions{})
	if pod.DeletionTimestamp != nil {
		tCtx.Fatalf("Victim pod was incorrectly marked for disruption during PodGroup preemption check (DeletionTimestamp set)")
	}
	_, cond := podutil.GetPodCondition(&pod.Status, v1.DisruptionTarget)
	if cond != nil {
		tCtx.Fatalf("Victim pod was incorrectly marked for disruption during PodGroup preemption check (DisruptionTarget condition set)")
	}
}

// testConsumableCapacityPreemption verifies that the scheduler can perform minimal preemption
// when using consumable capacity. It schedules multiple low-priority pods sharing a device,
// and ensures that a high-priority pod only preempts ENOUGH victims to satisfy its request,
// leaving others running.
func testConsumableCapacityPreemption(tCtx ktesting.TContext) {
	namespace := createTestNamespace(tCtx, nil)

	class, driverName := createTestClass(tCtx, namespace)

	// Create a ResourceSlice with a single device with a capacity of 10 units.
	capacityName := resourceapi.QualifiedName("example.com/cpus")
	slice := st.MakeResourceSlice("worker-0", driverName).Device("device-1",
		map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
			capacityName: {Value: resource.MustParse("10")},
		},
	).Obj()
	trueVal := true
	slice.Spec.Devices[0].AllowMultipleAllocations = &trueVal
	createSlice(tCtx, slice)

	createPriorityClass(tCtx, "low-priority", 100)
	createPriorityClass(tCtx, "high-priority", 200)

	// Claim for victims requesting 4 units
	victimClaim := st.MakeResourceClaim().Name("claim").Request(class.Name).Obj()
	victimClaim.Spec.Devices.Requests[0].Exactly.Capacity = &resourceapi.CapacityRequirements{
		Requests: map[resourceapi.QualifiedName]resource.Quantity{
			capacityName: resource.MustParse("4"),
		},
	}

	// Create Victim A
	victimClaimA := createClaim(tCtx, namespace, "-victim-a", class, victimClaim)
	victimPodA := podWithClaimName.DeepCopy()
	victimPodA.Spec.PriorityClassName = "low-priority"
	victimA := createPod(tCtx, namespace, "-victim-a", victimPodA, victimClaimA)
	waitForPodScheduled(tCtx, namespace, victimA.Name)

	// Create Victim B
	victimClaimB := createClaim(tCtx, namespace, "-victim-b", class, victimClaim)
	victimPodB := podWithClaimName.DeepCopy()
	victimPodB.Spec.PriorityClassName = "low-priority"
	victimB := createPod(tCtx, namespace, "-victim-b", victimPodB, victimClaimB)
	waitForPodScheduled(tCtx, namespace, victimB.Name)

	// Prototype for preemptor claim requesting 6 units
	preemptorClaim := st.MakeResourceClaim().Name("claim").Request(class.Name).Obj()
	preemptorClaim.Spec.Devices.Requests[0].Exactly.Capacity = &resourceapi.CapacityRequirements{
		Requests: map[resourceapi.QualifiedName]resource.Quantity{
			capacityName: resource.MustParse("6"),
		},
	}

	// Create preemptor claim
	preemptorClaim = createClaim(tCtx, namespace, "-preemptor", class, preemptorClaim)
	preemptorPod := podWithClaimName.DeepCopy()
	preemptorPod.Spec.PriorityClassName = "high-priority"
	preemptor := createPod(tCtx, namespace, "-preemptor", preemptorPod, preemptorClaim)

	// Preemptor should trigger preemption and get scheduled
	waitForPodScheduled(tCtx, namespace, preemptor.Name)

	// Verify that exactly one of the victims is preempted
	tCtx.Eventually(func(tCtx ktesting.TContext) int {
		podA, errA := tCtx.Client().CoreV1().Pods(namespace).Get(tCtx, victimA.Name, metav1.GetOptions{})
		podB, errB := tCtx.Client().CoreV1().Pods(namespace).Get(tCtx, victimB.Name, metav1.GetOptions{})
		if errA != nil || errB != nil {
			return 0
		}
		
		preemptedCount := 0
		var preemptedPod *v1.Pod
		if podA.DeletionTimestamp != nil {
			preemptedCount++
			preemptedPod = podA
		}
		if podB.DeletionTimestamp != nil {
			preemptedCount++
			preemptedPod = podB
		}
		
		if preemptedCount != 1 {
			return 0
		}
		
		_, cond := podutil.GetPodCondition(&preemptedPod.Status, v1.DisruptionTarget)
		if cond != nil && cond.Status == v1.ConditionTrue {
			return 1
		}
		return 0
	}).WithTimeout(30 * time.Second).Should(gomega.Equal(1), "Exactly one victim pod should be preempted with DisruptionTarget condition")
}

// testPartitionableDevicesPreemption verifies that preempting a partition of a device
// does not incorrectly release the entire parent device or affect sibling partitions.
func testPartitionableDevicesPreemption(tCtx ktesting.TContext) {
	// TODO: Implement preemption simulation test for claims requesting PartitionableDevices.
	// 
	// Importance: The DRAPartitionableDevices feature allows a single Admin device to be split into sub-devices
	// or partitions to serve multiple ExactCount requests. Preempting one of these partitions should not incorrectly
	// release the entire parent device, which would prematurely break sibling partitions.
	//
	// Design: Create an admin device. Have two low-priority victim pods claim distinct partitions from it.
	// Schedule a high-priority preemptor pod that requests one partition. Target one of the low-priority pods for eviction.
	// Validate the preemption plugin logic only releases the sub-device partitions assigned to the specific victim claim.
}

// testBindingConditionsPreemption verifies that preemption correctly handles claims
// with binding conditions, ensuring state is cleared and timeouts are respected.
func testBindingConditionsPreemption(tCtx ktesting.TContext) {
	// TODO: Implement preemption simulation test handling BindingConditions.
	//
	// Importance: Late-bound devices might require specific node-level preparation before the Pod can be scheduled.
	// The DRADeviceBindingConditions feature ensures that preemption correctly clears the Allocation state and
	// doesn't indefinitely wait for BindingCondition signals on devices simulating a pod removal.
	//
	// Design: Create a scenario using a ResourceSlice where devices demand BindingConditions.
	// Emulate delayed device preparation on the cluster. Evict the victim pod and preempt the resources.
	// Assert the scheduler respects the BindingTimeout context boundaries.
}

func createPriorityClass(tCtx ktesting.TContext, name string, value int32) {
	tCtx.Helper()
	pc := &schedulingv1.PriorityClass{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Value:      value,
	}
	_, err := tCtx.Client().SchedulingV1().PriorityClasses().Create(tCtx, pc, metav1.CreateOptions{})
	tCtx.ExpectNoError(err, "create priority class")
	tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
		tCtx.Log("Cleaning up PriorityClass...")
		deleteAndWait(tCtx, tCtx.Client().SchedulingV1().PriorityClasses().Delete, tCtx.Client().SchedulingV1().PriorityClasses().Get, pc.Name)
	})
}

func populatePodClaimStatus(tCtx ktesting.TContext, namespace string, pod *v1.Pod, claim *resourceapi.ResourceClaim, nodeName string) {
	tCtx.Helper()

	podStatus := pod.DeepCopy()
	podStatus.Status.ResourceClaimStatuses = []v1.PodResourceClaimStatus{
		{
			Name:              pod.Spec.ResourceClaims[0].Name,
			ResourceClaimName: &claim.Name,
		},
	}
	_, err := tCtx.Client().CoreV1().Pods(namespace).UpdateStatus(tCtx, podStatus, metav1.UpdateOptions{})
	tCtx.ExpectNoError(err, "update victim status")
}


