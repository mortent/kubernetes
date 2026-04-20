package dynamicresources

import (
	"context"
	"testing"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/dynamic-resource-allocation/structured"
	"k8s.io/kubernetes/test/utils/ktesting"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

func TestRemovePod(t *testing.T) {
	tCtx := ktesting.Init(t)

	claimName1 := "claim-1"

	claim1 := st.MakeResourceClaim().Namespace("default").Name(claimName1).Obj()
	claim1.Status.Allocation = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{
				{Driver: "test-driver", Pool: "pool-1", Device: "device-1"},
			},
		},
	}

	// Shared claim setup
	sharedClaimName := "shared-claim"
	sharedClaim := st.MakeResourceClaim().Namespace("default").Name(sharedClaimName).Obj()
	sharedClaim.Status.ReservedFor = []resourceapi.ResourceClaimConsumerReference{
		{Resource: "pods", UID: "pod-a-uid"},
		{Resource: "pods", UID: "pod-b-uid"},
	}

	podA := st.MakePod().Name("pod-a").Namespace("default").UID("pod-a-uid").Obj()
	podA.Spec.ResourceClaims = []v1.PodResourceClaim{{Name: "my-claim", ResourceClaimName: &claimName1}}
	podInfoA, _ := framework.NewPodInfo(podA)

	podSharedA := st.MakePod().Name("pod-shared-a").Namespace("default").UID("pod-a-uid").Obj()
	podSharedA.Spec.ResourceClaims = []v1.PodResourceClaim{{Name: "my-claim", ResourceClaimName: &sharedClaimName}}
	podInfoSharedA, _ := framework.NewPodInfo(podSharedA)

	podSharedB := st.MakePod().Name("pod-shared-b").Namespace("default").UID("pod-b-uid").Obj()
	podSharedB.Spec.ResourceClaims = []v1.PodResourceClaim{{Name: "my-claim", ResourceClaimName: &sharedClaimName}}
	podInfoSharedB, _ := framework.NewPodInfo(podSharedB)

	podGroupClaimName := "pod-group-claim"
	podGroupClaim := st.MakeResourceClaim().Namespace("default").Name(podGroupClaimName).Obj()
	podGroupClaim.Status.ReservedFor = []resourceapi.ResourceClaimConsumerReference{
		{APIGroup: "scheduling.k8s.io", Resource: "podgroups", Name: "my-group", UID: "group-uid"},
	}

	podGroupA := st.MakePod().Name("pod-group-a").Namespace("default").UID("pod-group-a-uid").Obj()
	podGroupA.Spec.ResourceClaims = []v1.PodResourceClaim{{Name: "my-claim", ResourceClaimName: &podGroupClaimName}}
	podInfoGroupA, _ := framework.NewPodInfo(podGroupA)

	// Spurious shared claim simulation setup
	uidMismatchClaimName := "spurious-claim"
	uidMismatchClaim := st.MakeResourceClaim().Namespace("default").Name(uidMismatchClaimName).Obj()
	uidMismatchClaim.Status.ReservedFor = []resourceapi.ResourceClaimConsumerReference{
		{Resource: "pods", UID: "never-simulated-pod-uid"},
	}

	podSpurious := st.MakePod().Name("pod-spurious").Namespace("default").UID("pod-spurious-uid").Obj()
	podSpurious.Spec.ResourceClaims = []v1.PodResourceClaim{{Name: "my-claim", ResourceClaimName: &uidMismatchClaimName}}
	podInfoSpurious, _ := framework.NewPodInfo(podSpurious)

	podNilClaim := st.MakePod().Name("pod-nil-claim").Namespace("default").UID("pod-nil-uid").Obj()
	templateName := "my-template"
	podNilClaim.Spec.ResourceClaims = []v1.PodResourceClaim{{Name: "my-claim", ResourceClaimTemplateName: &templateName}}
	podInfoNilClaim, _ := framework.NewPodInfo(podNilClaim)

	podDuplicateClaim := st.MakePod().Name("pod-dup-claim").Namespace("default").UID("pod-dup-uid").Obj()
	podDuplicateClaim.Spec.ResourceClaims = []v1.PodResourceClaim{
		{Name: "my-claim-1", ResourceClaimName: &claimName1},
		{Name: "my-claim-2", ResourceClaimName: &claimName1},
	}
	podInfoDuplicateClaim, _ := framework.NewPodInfo(podDuplicateClaim)

	missingClaimName := "missing-claim"
	podMissingClaim := st.MakePod().Name("pod-missing-claim").Namespace("default").UID("pod-missing-uid").Obj()
	podMissingClaim.Spec.ResourceClaims = []v1.PodResourceClaim{{Name: "my-claim", ResourceClaimName: &missingClaimName}}
	podInfoMissingClaim, _ := framework.NewPodInfo(podMissingClaim)

	podDirect := st.MakePod().Name("pod-direct").Namespace("default").UID("pod-direct-uid").Obj()
	podDirect.Spec.ResourceClaims = []v1.PodResourceClaim{{Name: "my-claim", ResourceClaimName: &claimName1}}
	podInfoDirect, _ := framework.NewPodInfo(podDirect)

	testCases := []struct {
		name         string
		podInfo      fwk.PodInfo
		claims       map[string]*resourceapi.ResourceClaim
		initialState *stateData
		verify       func(t *testing.T, state *stateData, status *fwk.Status)
	}{
		{
			name:    "single-user-claim-released",
			podInfo: podInfoA,
			claims:  map[string]*resourceapi.ResourceClaim{claimName1: claim1},
			initialState: &stateData{
				simulatedReleasedClaims: sets.New[string](),
			},
			verify: func(t *testing.T, state *stateData, status *fwk.Status) {
				if status != nil {
					t.Errorf("expected no error, got %v", status)
				}
				if !state.simulatedReleasedClaims.Has(claimName1) {
					t.Errorf("expected %s to be released", claimName1)
				}
			},
		},
		{
			name:    "shared-claim-single-user-out-not-released",
			podInfo: podInfoSharedA,
			claims:  map[string]*resourceapi.ResourceClaim{sharedClaimName: sharedClaim},
			initialState: &stateData{
				simulatedReleasedClaims:       sets.New[string](),
				simulatedRemovedUsersForClaim: make(map[string]sets.Set[types.UID]),
			},
			verify: func(t *testing.T, state *stateData, status *fwk.Status) {
				if status != nil {
					t.Errorf("expected no error, got %v", status)
				}
				if state.simulatedReleasedClaims.Has(sharedClaimName) {
					t.Errorf("expected %s NOT to be released", sharedClaimName)
				}
				if users, ok := state.simulatedRemovedUsersForClaim[sharedClaimName]; !ok || !users.Has("pod-a-uid") {
					t.Errorf("expected pod-a-uid to be recorded as removed")
				}
			},
		},
		{
			name:    "shared-claim-all-users-out-released",
			podInfo: podInfoSharedB,
			claims:  map[string]*resourceapi.ResourceClaim{sharedClaimName: sharedClaim},
			initialState: &stateData{
				simulatedReleasedClaims:       sets.New[string](),
				simulatedRemovedUsersForClaim: map[string]sets.Set[types.UID]{sharedClaimName: sets.New[types.UID]("pod-a-uid")},
			},
			verify: func(t *testing.T, state *stateData, status *fwk.Status) {
				if status != nil {
					t.Errorf("expected no error, got %v", status)
				}
				if !state.simulatedReleasedClaims.Has(sharedClaimName) {
					t.Errorf("expected %s to be released", sharedClaimName)
				}
			},
		},
		{
			name:    "pod-group-claim-not-released",
			podInfo: podInfoGroupA,
			claims:  map[string]*resourceapi.ResourceClaim{podGroupClaimName: podGroupClaim},
			initialState: &stateData{
				simulatedReleasedClaims:       sets.New[string](),
				simulatedRemovedUsersForClaim: make(map[string]sets.Set[types.UID]),
			},
			verify: func(t *testing.T, state *stateData, status *fwk.Status) {
				if status != nil {
					t.Errorf("expected no error, got %v", status)
				}
				if state.simulatedReleasedClaims.Has(podGroupClaimName) {
					t.Errorf("expected %s NOT to be released because it is held by a PodGroup", podGroupClaimName)
				}
				if users, ok := state.simulatedRemovedUsersForClaim[podGroupClaimName]; !ok || !users.Has("pod-group-a-uid") {
					t.Errorf("expected pod-group-a-uid to be recorded as removed")
				}
			},
		},
		{
			name:    "shared-claim-count-matches-but-uids-differ-not-released",
			podInfo: podInfoSpurious,
			claims:  map[string]*resourceapi.ResourceClaim{uidMismatchClaimName: uidMismatchClaim},
			initialState: &stateData{
				simulatedReleasedClaims:       sets.New[string](),
				simulatedRemovedUsersForClaim: make(map[string]sets.Set[types.UID]),
			},
			verify: func(t *testing.T, state *stateData, status *fwk.Status) {
				if status != nil {
					t.Errorf("expected no error, got %v", status)
				}
				if state.simulatedReleasedClaims.Has(uidMismatchClaimName) {
					t.Errorf("expected %s NOT to be released because the simulated UIDs do not match ReservedFor", uidMismatchClaimName)
				}
				if users, ok := state.simulatedRemovedUsersForClaim[uidMismatchClaimName]; !ok || !users.Has("pod-spurious-uid") {
					t.Errorf("expected pod-spurious-uid to be recorded as removed")
				}
			},
		},
		{
			name:    "duplicate-claim-name-ignored",
			podInfo: podInfoDuplicateClaim, // references claimName1 twice
			claims:  map[string]*resourceapi.ResourceClaim{claimName1: claim1},
			initialState: &stateData{
				simulatedReleasedClaims:       sets.New[string](),
				simulatedRemovedUsersForClaim: make(map[string]sets.Set[types.UID]),
				allocatedState: &structured.AllocatedState{
					AllocatedDevices:         sets.New[structured.DeviceID](structured.MakeDeviceID("test-driver", "pool-1", "device-1")),
					AllocatedSharedDeviceIDs: sets.New[structured.SharedDeviceID](),
					AggregatedCapacity:       structured.NewConsumedCapacityCollection(),
				},
			},
			verify: func(t *testing.T, state *stateData, status *fwk.Status) {
				if status != nil {
					t.Errorf("expected no error, got %v", status)
				}
				if users, ok := state.simulatedRemovedUsersForClaim[claimName1]; !ok || users.Len() != 1 {
					t.Errorf("expected pod-dup-uid to be recorded exactly once, got %v users", users.Len())
				}
				if !state.simulatedReleasedClaims.Has(claimName1) {
					t.Errorf("expected %s to be released", claimName1)
				}
				if state.allocatedState.AllocatedDevices.Len() != 0 {
					t.Errorf("expected devices to be removed strictly once")
				}
			},
		},
		{
			name:    "missing-claim-ignored",
			podInfo: podInfoMissingClaim,
			claims:  map[string]*resourceapi.ResourceClaim{},
			initialState: &stateData{
				simulatedReleasedClaims:       sets.New[string](),
				simulatedRemovedUsersForClaim: make(map[string]sets.Set[types.UID]),
			},
			verify: func(t *testing.T, state *stateData, status *fwk.Status) {
				if status != nil {
					t.Errorf("expected no error, got %v", status)
				}
				if users, ok := state.simulatedRemovedUsersForClaim[missingClaimName]; !ok || !users.Has("pod-missing-uid") {
					t.Errorf("expected pod-missing-uid to be recorded as removed")
				}
			},
		},
		{
			name:    "nil-resource-claim-name-ignored",
			podInfo: podInfoNilClaim, // References nil name
			claims:  map[string]*resourceapi.ResourceClaim{},
			initialState: &stateData{
				simulatedReleasedClaims:       sets.New[string](),
				simulatedRemovedUsersForClaim: make(map[string]sets.Set[types.UID]),
			},
			verify: func(t *testing.T, state *stateData, status *fwk.Status) {
				if status != nil {
					t.Errorf("expected no error, got %v", status)
				}
				if len(state.simulatedRemovedUsersForClaim) != 0 {
					t.Errorf("expected no removal tracking, got %v", state.simulatedRemovedUsersForClaim)
				}
			},
		},
		{
			name:    "direct-claim-released",
			podInfo: podInfoDirect,
			claims:  map[string]*resourceapi.ResourceClaim{claimName1: claim1},
			initialState: &stateData{
				simulatedReleasedClaims: sets.New[string](),
			},
			verify: func(t *testing.T, state *stateData, status *fwk.Status) {
				if status != nil {
					t.Errorf("expected no error, got %v", status)
				}
				if !state.simulatedReleasedClaims.Has(claimName1) {
					t.Errorf("expected %s to be released", claimName1)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockManager := &mockPreemptionDRAManager{claims: tc.claims}
			pl := &DynamicResources{draManager: mockManager}

			cycleState := framework.NewCycleState()
			cycleState.Write(stateKey, tc.initialState)

			nodeInfo := framework.NewNodeInfo()

			status := pl.RemovePod(tCtx, cycleState, nil, tc.podInfo, nodeInfo)

			tc.verify(t, tc.initialState, status)
		})
	}
}

func TestAddPod(t *testing.T) {
	tCtx := ktesting.Init(t)

	claimName1 := "claim-1"
	claim1 := st.MakeResourceClaim().Namespace("default").Name(claimName1).Obj()
	claim1.Status.Allocation = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{
				{Driver: "test-driver", Pool: "pool-1", Device: "device-1"},
			},
		},
	}

	podA := st.MakePod().Name("pod-a").Namespace("default").UID("pod-a-uid").Obj()
	podA.Spec.ResourceClaims = []v1.PodResourceClaim{{Name: "my-claim", ResourceClaimName: &claimName1}}
	podInfoA, _ := framework.NewPodInfo(podA)

	podNilClaim := st.MakePod().Name("pod-nil-claim").Namespace("default").UID("pod-nil-uid").Obj()
	templateName := "my-template"
	podNilClaim.Spec.ResourceClaims = []v1.PodResourceClaim{{Name: "my-claim", ResourceClaimTemplateName: &templateName}}
	podInfoNilClaim, _ := framework.NewPodInfo(podNilClaim)

	podDuplicateClaim := st.MakePod().Name("pod-dup-claim").Namespace("default").UID("pod-dup-uid").Obj()
	podDuplicateClaim.Spec.ResourceClaims = []v1.PodResourceClaim{
		{Name: "my-claim-1", ResourceClaimName: &claimName1},
		{Name: "my-claim-2", ResourceClaimName: &claimName1},
	}
	podInfoDuplicateClaim, _ := framework.NewPodInfo(podDuplicateClaim)

	missingClaimName := "missing-claim"
	podMissingClaim := st.MakePod().Name("pod-missing-claim").Namespace("default").UID("pod-missing-uid").Obj()
	podMissingClaim.Spec.ResourceClaims = []v1.PodResourceClaim{{Name: "my-claim", ResourceClaimName: &missingClaimName}}
	podInfoMissingClaim, _ := framework.NewPodInfo(podMissingClaim)

	podDirect := st.MakePod().Name("pod-direct").Namespace("default").UID("pod-direct-uid").Obj()
	podDirect.Spec.ResourceClaims = []v1.PodResourceClaim{{Name: "my-claim", ResourceClaimName: &claimName1}}
	podInfoDirect, _ := framework.NewPodInfo(podDirect)

	testCases := []struct {
		name         string
		podInfo      fwk.PodInfo
		claims       map[string]*resourceapi.ResourceClaim
		initialState *stateData
		verify       func(t *testing.T, state *stateData, status *fwk.Status)
	}{
		{
			name:    "released-claim-readded",
			podInfo: podInfoA,
			claims:  map[string]*resourceapi.ResourceClaim{claimName1: claim1},
			initialState: &stateData{
				simulatedReleasedClaims: sets.New[string](claimName1),
				allocatedState: &structured.AllocatedState{
					AllocatedDevices:         sets.New[structured.DeviceID](),
					AllocatedSharedDeviceIDs: sets.New[structured.SharedDeviceID](),
					AggregatedCapacity:       structured.NewConsumedCapacityCollection(),
				},
			},
			verify: func(t *testing.T, state *stateData, status *fwk.Status) {
				if status != nil {
					t.Errorf("expected no error, got %v", status)
				}
				if state.simulatedReleasedClaims.Has(claimName1) {
					t.Errorf("expected %s to be removed from released claims", claimName1)
				}
				if state.allocatedState.AllocatedDevices.Len() == 0 {
					t.Errorf("expected devices to be restored to allocatedState")
				}
			},
		},

		{
			name:    "shared-claim-clear-counter",
			podInfo: podInfoA,
			claims:  map[string]*resourceapi.ResourceClaim{claimName1: claim1},
			initialState: &stateData{
				simulatedReleasedClaims:       sets.New[string](),
				simulatedRemovedUsersForClaim: map[string]sets.Set[types.UID]{claimName1: sets.New[types.UID]("pod-a-uid")},
			},
			verify: func(t *testing.T, state *stateData, status *fwk.Status) {
				if status != nil {
					t.Errorf("expected no error, got %v", status)
				}
				if users, ok := state.simulatedRemovedUsersForClaim[claimName1]; ok && users.Has("pod-a-uid") {
					t.Errorf("expected pod-a-uid to be cleared from simulatedRemovedUsersForClaim")
				}
			},
		},
		{
			name:    "duplicate-claim-name-ignored",
			podInfo: podInfoDuplicateClaim,
			claims:  map[string]*resourceapi.ResourceClaim{claimName1: claim1},
			initialState: &stateData{
				simulatedReleasedClaims:       sets.New[string](claimName1),
				simulatedRemovedUsersForClaim: map[string]sets.Set[types.UID]{claimName1: sets.New[types.UID]("pod-dup-uid")},
				allocatedState: &structured.AllocatedState{
					AllocatedDevices:         sets.New[structured.DeviceID](),
					AllocatedSharedDeviceIDs: sets.New[structured.SharedDeviceID](),
					AggregatedCapacity:       structured.NewConsumedCapacityCollection(),
				},
			},
			verify: func(t *testing.T, state *stateData, status *fwk.Status) {
				if status != nil {
					t.Errorf("expected no error, got %v", status)
				}
				if state.simulatedReleasedClaims.Has(claimName1) {
					t.Errorf("expected %s to be removed from released claims strictly once", claimName1)
				}
				if users, ok := state.simulatedRemovedUsersForClaim[claimName1]; ok && users.Has("pod-dup-uid") {
					t.Errorf("expected pod-dup-uid to be completely cleared exactly once")
				}
				if state.allocatedState.AllocatedDevices.Len() != 1 {
					t.Errorf("expected devices to be strictly restored only once")
				}
			},
		},
		{
			name:    "missing-claim-ignored",
			podInfo: podInfoMissingClaim,
			claims:  map[string]*resourceapi.ResourceClaim{},
			initialState: &stateData{
				simulatedReleasedClaims:       sets.New[string](missingClaimName),
				simulatedRemovedUsersForClaim: map[string]sets.Set[types.UID]{missingClaimName: sets.New[types.UID]("pod-missing-uid")},
				allocatedState: &structured.AllocatedState{
					AllocatedDevices:         sets.New[structured.DeviceID](),
					AllocatedSharedDeviceIDs: sets.New[structured.SharedDeviceID](),
					AggregatedCapacity:       structured.NewConsumedCapacityCollection(),
				},
			},
			verify: func(t *testing.T, state *stateData, status *fwk.Status) {
				if status != nil {
					t.Errorf("expected no error, got %v", status)
				}
				if state.simulatedReleasedClaims.Has(missingClaimName) {
					t.Errorf("expected %s to be normally evaluated and popped from map", missingClaimName)
				}
				if users, ok := state.simulatedRemovedUsersForClaim[missingClaimName]; ok && users.Has("pod-missing-uid") {
					t.Errorf("expected pod-missing-uid to normally clear from tracking")
				}
			},
		},
		{
			name:    "nil-resource-claim-name-ignored",
			podInfo: podInfoNilClaim, // References nil name
			claims:  map[string]*resourceapi.ResourceClaim{},
			initialState: &stateData{
				simulatedReleasedClaims:  sets.New[string](),
			},
			verify: func(t *testing.T, state *stateData, status *fwk.Status) {
				if status != nil {
					t.Errorf("expected no error, got %v", status)
				}
			},
		},
		{
			name:    "direct-claim-readded",
			podInfo: podInfoDirect,
			claims:  map[string]*resourceapi.ResourceClaim{claimName1: claim1},
			initialState: &stateData{
				simulatedReleasedClaims: sets.New[string](claimName1),
				allocatedState: &structured.AllocatedState{
					AllocatedDevices:         sets.New[structured.DeviceID](),
					AllocatedSharedDeviceIDs: sets.New[structured.SharedDeviceID](),
					AggregatedCapacity:       structured.NewConsumedCapacityCollection(),
				},
			},
			verify: func(t *testing.T, state *stateData, status *fwk.Status) {
				if status != nil {
					t.Errorf("expected no error, got %v", status)
				}
				if state.simulatedReleasedClaims.Has(claimName1) {
					t.Errorf("expected %s to be removed from released claims", claimName1)
				}
				if state.allocatedState.AllocatedDevices.Len() == 0 {
					t.Errorf("expected devices to be restored to allocatedState")
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockManager := &mockPreemptionDRAManager{claims: tc.claims}
			pl := &DynamicResources{draManager: mockManager}

			cycleState := framework.NewCycleState()
			cycleState.Write(stateKey, tc.initialState)

			nodeInfo := framework.NewNodeInfo()

			status := pl.AddPod(tCtx, cycleState, nil, tc.podInfo, nodeInfo)

			tc.verify(t, tc.initialState, status)
		})
	}
}

func TestSharedClaimsPreemption(t *testing.T) {
	// Create a mock SharedDRAManager that returns a shared claim.
	claimName := "claim-1"
	claim := st.MakeResourceClaim().Namespace("default").Name(claimName).Obj()
	claim.Status.ReservedFor = []resourceapi.ResourceClaimConsumerReference{
		{Resource: "pods", UID: "victim-1-uid"},
		{Resource: "pods", UID: "victim-2-uid"},
	}

	mockManager := &mockPreemptionDRAManager{
		claims: map[string]*resourceapi.ResourceClaim{
			claimName: claim,
		},
	}

	pl := &DynamicResources{
		draManager: mockManager,
	}

	state := &stateData{
		simulatedReleasedClaims:       sets.New[string](),
		simulatedRemovedUsersForClaim: make(map[string]sets.Set[types.UID]),
	}
	cycleState := framework.NewCycleState()
	cycleState.Write(stateKey, state)

	// Victim 1
	pod1 := st.MakePod().Name("victim-1").Namespace("default").UID("victim-1-uid").Obj()
	pod1.Spec.ResourceClaims = []v1.PodResourceClaim{
		{
			Name:              "my-claim",
			ResourceClaimName: &claimName,
		},
	}
	podInfo1, err := framework.NewPodInfo(pod1)
	if err != nil {
		t.Fatalf("NewPodInfo failed: %v", err)
	}

	// Victim 2
	pod2 := st.MakePod().Name("victim-2").Namespace("default").UID("victim-2-uid").Obj()
	pod2.Spec.ResourceClaims = []v1.PodResourceClaim{
		{
			Name:              "my-claim",
			ResourceClaimName: &claimName,
		},
	}
	podInfo2, err := framework.NewPodInfo(pod2)
	if err != nil {
		t.Fatalf("NewPodInfo failed: %v", err)
	}

	nodeInfo := framework.NewNodeInfo()

	// 1. Remove Victim 1. Claim should NOT be released.
	status := pl.RemovePod(context.Background(), cycleState, nil, podInfo1, nodeInfo)
	if status != nil {
		t.Errorf("unexpected status %v", status)
	}
	if state.simulatedReleasedClaims.Has(claimName) {
		t.Errorf("expected claim-1 NOT to be marked as released after removing only victim-1")
	}

	// 2. Remove Victim 2. Claim SHOULD be released!
	status = pl.RemovePod(context.Background(), cycleState, nil, podInfo2, nodeInfo)
	if status != nil {
		t.Errorf("unexpected status %v", status)
	}
	if !state.simulatedReleasedClaims.Has(claimName) {
		t.Errorf("expected claim-1 to be marked as released after removing both victims")
	}
}

type mockPreemptionDRAManager struct {
	fwk.SharedDRAManager
	claims map[string]*resourceapi.ResourceClaim
}

func (m *mockPreemptionDRAManager) ResourceClaims() fwk.ResourceClaimTracker {
	return &mockPreemptionClaimTracker{claims: m.claims}
}

type mockPreemptionClaimTracker struct {
	fwk.ResourceClaimTracker
	claims map[string]*resourceapi.ResourceClaim
}

func (t *mockPreemptionClaimTracker) Get(namespace, name string) (*resourceapi.ResourceClaim, error) {
	if claim, ok := t.claims[name]; ok {
		return claim, nil
	}
	return nil, apierrors.NewNotFound(resourceapi.Resource("resourceclaims"), name)
}

func TestPreemptionEndToEnd(t *testing.T) {
	tCtx := ktesting.Init(t)

	// Create test-specific class
	testClassName := "test-class"
	testClass := &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{Name: testClassName},
	}

	// Create a slice with 3 devices on "test-driver"
	testSlice := st.MakeResourceSlice("node-1", "test-driver").
		Device("dev-1").
		Device("dev-2").
		Device("dev-3").
		Obj()

	// Preemptor claims (unallocated)
	preemptorClaim1 := st.MakeResourceClaim().Namespace("default").Name("preemptor-claim-1").Request(testClassName).Obj()
	preemptorClaim2 := st.MakeResourceClaim().Namespace("default").Name("preemptor-claim-2").Request(testClassName).Obj()

	// Victim claims (allocated)
	makeAlloc := func(devName string) *resourceapi.AllocationResult {
		return &resourceapi.AllocationResult{
			Devices: resourceapi.DeviceAllocationResult{
				Results: []resourceapi.DeviceRequestAllocationResult{
					{
						Driver:  "test-driver",
						Pool:    "node-1",
						Device:  devName,
						Request: "req",
					},
				},
			},
		}
	}

	victimClaim1 := preemptorClaim1.DeepCopy()
	victimClaim1.Name = "victim-claim-1"
	victimClaim1.Status.Allocation = makeAlloc("dev-1")

	victimSharedClaim := preemptorClaim1.DeepCopy()
	victimSharedClaim.Name = "victim-shared-claim"
	victimSharedClaim.Status.Allocation = makeAlloc("dev-2")
	victimSharedClaim.Status.ReservedFor = []resourceapi.ResourceClaimConsumerReference{
		{Resource: "pods", UID: "victim-shared-a-uid"},
		{Resource: "pods", UID: "victim-shared-b-uid"},
	}

	victimClaim2 := preemptorClaim1.DeepCopy()
	victimClaim2.Name = "victim-claim-2"
	victimClaim2.Status.Allocation = makeAlloc("dev-3")

	mockManager := &mockPreemptionDRAManagerWithSlices{
		claims: map[string]*resourceapi.ResourceClaim{
			victimClaim1.Name:      victimClaim1,
			victimSharedClaim.Name: victimSharedClaim,
			victimClaim2.Name:      victimClaim2,
		},
	}

	makeVictim := func(name, uid string, claimNames ...string) fwk.PodInfo {
		pod := st.MakePod().Name(name).Namespace("default").UID(uid).Obj()
		for _, cn := range claimNames {
			claimNameCopy := cn
			pod.Spec.ResourceClaims = append(pod.Spec.ResourceClaims, v1.PodResourceClaim{
				Name:              "claim-" + cn,
				ResourceClaimName: &claimNameCopy,
			})
		}
		pi, _ := framework.NewPodInfo(pod)
		return pi
	}

	victim1 := makeVictim("victim-1", "victim-1-uid", victimClaim1.Name)
	victimSharedA := makeVictim("victim-a", "victim-shared-a-uid", victimSharedClaim.Name)
	victimSharedB := makeVictim("victim-b", "victim-shared-b-uid", victimSharedClaim.Name)
	victim2 := makeVictim("victim-2", "victim-2-uid", victimClaim2.Name)
	victimMulti := makeVictim("victim-multi", "victim-multi-uid", victimClaim1.Name, victimClaim2.Name)

	type op struct {
		action string
		pod    fwk.PodInfo
	}

	testCases := []struct {
		name             string
		preemptorClaims  []*resourceapi.ResourceClaim
		ops              []op
		wantFilterStatus *fwk.Status
	}{
		{
			name:             "baseline-exhaustion-no-preemption",
			preemptorClaims:  []*resourceapi.ResourceClaim{preemptorClaim1},
			ops:              []op{},
			wantFilterStatus: fwk.NewStatus(fwk.Unschedulable, "cannot allocate all claims"),
		},
		{
			name:            "successful-preemption-single-victim",
			preemptorClaims: []*resourceapi.ResourceClaim{preemptorClaim1},
			ops: []op{
				{"remove", victim1}, // frees dev-1
			},
			wantFilterStatus: nil,
		},
		{
			name:            "partial-preemption-shared-claim-fails",
			preemptorClaims: []*resourceapi.ResourceClaim{preemptorClaim1},
			ops: []op{
				{"remove", victimSharedA}, // dev-2 not freed yet
			},
			wantFilterStatus: fwk.NewStatus(fwk.Unschedulable, "cannot allocate all claims"),
		},
		{
			name:            "full-preemption-shared-claim-succeeds",
			preemptorClaims: []*resourceapi.ResourceClaim{preemptorClaim1},
			ops: []op{
				{"remove", victimSharedA},
				{"remove", victimSharedB}, // frees dev-2
			},
			wantFilterStatus: nil,
		},
		{
			name:            "multi-claim-preemption",
			preemptorClaims: []*resourceapi.ResourceClaim{preemptorClaim1, preemptorClaim2},
			ops: []op{
				{"remove", victim1}, // frees dev-1
				{"remove", victim2}, // frees dev-3
			},
			wantFilterStatus: nil,
		},
		{
			name:            "preemption-rollback",
			preemptorClaims: []*resourceapi.ResourceClaim{preemptorClaim1},
			ops: []op{
				{"remove", victim1}, // frees dev-1
				{"add", victim1},    // takes back dev-1
			},
			wantFilterStatus: fwk.NewStatus(fwk.Unschedulable, "cannot allocate all claims"),
		},
		{
			name:            "single-victim-multiple-claims",
			preemptorClaims: []*resourceapi.ResourceClaim{preemptorClaim1, preemptorClaim2},
			ops: []op{
				{"remove", victimMulti}, // frees dev-1 and dev-3 simultaneously
			},
			wantFilterStatus: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			feats := feature.Features{}

			testCtx := setup(tCtx, nil, nil, nil, []*resourceapi.DeviceClass{testClass}, nil, []apiruntime.Object{testSlice}, feats, false, nil)
			pl := testCtx.p

			mockManager.SharedDRAManager = testCtx.draManager
			pl.draManager = mockManager
			pl.enabled = true

			state := &stateData{
				claims:                        newClaimStore(tc.preemptorClaims, nil, nil),
				informationsForClaim:          make([]informationForClaim, len(tc.preemptorClaims)),
				nodeAllocations:               make(map[string]nodeAllocation),
				simulatedReleasedClaims:       sets.New[string](),
				simulatedRemovedUsersForClaim: make(map[string]sets.Set[types.UID]),
				allocatedState: &structured.AllocatedState{
					AllocatedDevices:         sets.New[structured.DeviceID](),
					AllocatedSharedDeviceIDs: sets.New[structured.SharedDeviceID](),
					AggregatedCapacity:       structured.NewConsumedCapacityCollection(),
				},
			}

			// Pre-allocate the victim claims so they exhaust the devices (dev-1, dev-2, dev-3)
			pl.addClaimToAllocatedState(state.allocatedState, victimClaim1)
			pl.addClaimToAllocatedState(state.allocatedState, victimSharedClaim)
			pl.addClaimToAllocatedState(state.allocatedState, victimClaim2)

			testSlices, _ := testCtx.draManager.ResourceSlices().ListWithDeviceTaintRules()
			allocator, _ := structured.NewAllocator(tCtx, AllocatorFeatures(feats), *state.allocatedState, testCtx.draManager.DeviceClasses(), testSlices, nil)
			state.allocator = allocator

			cycleState := framework.NewCycleState()
			cycleState.Write(stateKey, state)

			node := st.MakeNode().Name("node-1").Obj()
			nodeInfo := framework.NewNodeInfo()
			nodeInfo.SetNode(node)

			preemptorPod := st.MakePod().Name("preemptor").Namespace("default").Obj()

			for _, op := range tc.ops {
				if op.action == "remove" {
					pl.RemovePod(tCtx, cycleState, preemptorPod, op.pod, nodeInfo)
				} else if op.action == "add" {
					pl.AddPod(tCtx, cycleState, preemptorPod, op.pod, nodeInfo)
				}
			}

			status := pl.Filter(tCtx, cycleState, preemptorPod, nodeInfo)

			if status.Code() != tc.wantFilterStatus.Code() {
				t.Errorf("expected status code %v, got %v", tc.wantFilterStatus.Code(), status.Code())
			}
			if tc.wantFilterStatus.Code() == fwk.Error || tc.wantFilterStatus.Code() == fwk.UnschedulableAndUnresolvable {
				if status.Message() != tc.wantFilterStatus.Message() {
					t.Errorf("expected status message %q, got %q", tc.wantFilterStatus.Message(), status.Message())
				}
			}
		})
	}
}

type mockPreemptionDRAManagerWithSlices struct {
	fwk.SharedDRAManager
	claims map[string]*resourceapi.ResourceClaim
}

func (m *mockPreemptionDRAManagerWithSlices) ResourceClaims() fwk.ResourceClaimTracker {
	return &mockPreemptionClaimTracker{claims: m.claims}
}

func TestPreemptionSimulationSymmetry(t *testing.T) {
	tCtx := ktesting.Init(t)

	// Setup mock manager and claims
	claimName := "claim-1"
	claim := st.MakeResourceClaim().Namespace("default").Name(claimName).Obj()
	claim.Status.ReservedFor = []resourceapi.ResourceClaimConsumerReference{
		{Resource: "pods", UID: "pod-a-uid"},
		{Resource: "pods", UID: "pod-b-uid"},
	}
	// Setup allocation status for testing symmetry of device removal/addition
	claim.Status.Allocation = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{
				{
					Driver: "test-driver",
					Pool:   "pool-1",
					Device: "device-1",
				},
			},
		},
	}

	mockManager := &mockPreemptionDRAManager{
		claims: map[string]*resourceapi.ResourceClaim{
			claimName: claim,
		},
	}

	pl := &DynamicResources{
		draManager: mockManager,
	}

	podA := st.MakePod().Name("pod-a").Namespace("default").UID("pod-a-uid").Obj()
	podA.Spec.ResourceClaims = []v1.PodResourceClaim{{Name: "my-claim", ResourceClaimName: &claimName}}
	podInfoA, _ := framework.NewPodInfo(podA)

	podB := st.MakePod().Name("pod-b").Namespace("default").UID("pod-b-uid").Obj()
	podB.Spec.ResourceClaims = []v1.PodResourceClaim{{Name: "my-claim", ResourceClaimName: &claimName}}
	podInfoB, _ := framework.NewPodInfo(podB)

	type op struct {
		action string // "remove", "add"
		pod    fwk.PodInfo
	}

	testCases := []struct {
		name   string
		ops    []op
		verify func(t *testing.T, state *stateData)
	}{
		{
			name: "single-pod-remove-add",
			ops: []op{
				{"remove", podInfoA},
				{"add", podInfoA},
			},
			verify: func(t *testing.T, state *stateData) {
				if state.simulatedReleasedClaims.Len() != 0 {
					t.Errorf("expected no released claims, got %d", state.simulatedReleasedClaims.Len())
				}
				if users, ok := state.simulatedRemovedUsersForClaim[claimName]; ok && users.Len() != 0 {
					t.Errorf("expected no removed users, got %v", users.Len())
				}
			},
		},
		{
			name: "shared-claims-remove-A-remove-B-add-A-add-B",
			ops: []op{
				{"remove", podInfoA},
				{"remove", podInfoB},
				{"add", podInfoA},
				{"add", podInfoB},
			},
			verify: func(t *testing.T, state *stateData) {
				if state.simulatedReleasedClaims.Len() != 0 {
					t.Errorf("expected no released claims, got %d", state.simulatedReleasedClaims.Len())
				}
				if users, ok := state.simulatedRemovedUsersForClaim[claimName]; ok && users.Len() != 0 {
					t.Errorf("expected no removed users, got %v", users.Len())
				}
			},
		},
		{
			name: "shared-claims-remove-A-remove-B-add-B-add-A",
			ops: []op{
				{"remove", podInfoA},
				{"remove", podInfoB},
				{"add", podInfoB},
				{"add", podInfoA},
			},
			verify: func(t *testing.T, state *stateData) {
				if state.simulatedReleasedClaims.Len() != 0 {
					t.Errorf("expected no released claims, got %d", state.simulatedReleasedClaims.Len())
				}
				if users, ok := state.simulatedRemovedUsersForClaim[claimName]; ok && users.Len() != 0 {
					t.Errorf("expected no removed users, got %v", users.Len())
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state := &stateData{
				simulatedReleasedClaims:       sets.New[string](),
				simulatedRemovedUsersForClaim: make(map[string]sets.Set[types.UID]),
				allocatedState: &structured.AllocatedState{
					AllocatedDevices:         sets.New[structured.DeviceID](),
					AllocatedSharedDeviceIDs: sets.New[structured.SharedDeviceID](),
					AggregatedCapacity:       structured.NewConsumedCapacityCollection(),
				},
			}
			cycleState := framework.NewCycleState()
			cycleState.Write(stateKey, state)

			nodeInfo := framework.NewNodeInfo()

			// Run Operations
			for _, operation := range tc.ops {
				if operation.action == "remove" {
					pl.RemovePod(tCtx, cycleState, nil, operation.pod, nodeInfo)
				} else if operation.action == "add" {
					pl.AddPod(tCtx, cycleState, nil, operation.pod, nodeInfo)
				}
			}

			// Verify Invariants after simulation completes
			tc.verify(t, state)
		})
	}
}
