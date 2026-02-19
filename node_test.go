package node

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/nikitakosatka/hive/pkg/failure"
	"github.com/nikitakosatka/hive/pkg/hive"
	"github.com/nikitakosatka/hive/pkg/network"
	timemodel "github.com/nikitakosatka/hive/pkg/time"
)

type send struct {
	From    string
	To      string
	Payload string
}

func TestReliableVectorClockNode_OrderAndReliability(t *testing.T) {
	t.Parallel()

	netCfg := network.NewFairLossNetwork(0.3, 0.3)
	timeCfg := timemodel.NewTime(
		timemodel.Synchronous,
		&timemodel.ConstantLatency{Latency: 5 * time.Millisecond},
		0.0,
	)
	failCfg := failure.NewCrashStop(0.0)
	config := hive.NewConfig(
		hive.WithNetwork(netCfg),
		hive.WithTime(timeCfg),
		hive.WithNodesFailures(failCfg),
	)

	tests := []struct {
		name                   string
		nodes                  []string
		phases                 [][]send
		expectedOrderChains    [][]string
		expectedParallelGroups [][]string
	}{
		{
			name:  "A_to_B_and_B_to_C_batch",
			nodes: []string{"A", "B", "C"},
			phases: func() [][]send {
				phases := make([][]send, 0, 20)
				for i := range 20 {
					phases = append(phases, []send{
						{From: "A", To: "B", Payload: fmt.Sprintf("A->B-%d", i)},
						{From: "B", To: "C", Payload: fmt.Sprintf("B->C-%d", i)},
					})
				}
				return phases
			}(),
			expectedOrderChains: nil,
			expectedParallelGroups: func() [][]string {
				parallel := make([][]string, 0)
				for i := range 20 {
					for j := range i {
						parallel = append(parallel, make([]string, 0))
						parallel[len(parallel)-1] = append(parallel[len(parallel)-1], fmt.Sprintf("A->B-%d", i))
						parallel[len(parallel)-1] = append(parallel[len(parallel)-1], fmt.Sprintf("B->C-%d", j))
					}
				}
				return parallel
			}(),
		},
		{
			name:  "three_messages_m1_then_m2_then_m3",
			nodes: []string{"A", "B", "C"},
			phases: [][]send{
				{{From: "A", To: "B", Payload: "m1"}},
				{{From: "B", To: "C", Payload: "m2"}},
				{{From: "A", To: "C", Payload: "m3"}},
			},
			expectedOrderChains:    [][]string{{"m1", "m2"}, {"m1", "m3"}},
			expectedParallelGroups: [][]string{{"m2", "m3"}},
		},
		{
			name:  "per_node_order_respected",
			nodes: []string{"A", "B"},
			phases: [][]send{
				{{From: "A", To: "B", Payload: "first"}},
				{{From: "A", To: "B", Payload: "second"}},
			},
			expectedOrderChains:    [][]string{{"first", "second"}},
			expectedParallelGroups: nil,
		},
		{
			name:  "linear_chain_two_hops",
			nodes: []string{"A", "B", "C"},
			phases: [][]send{
				{{From: "A", To: "B", Payload: "step1"}},
				{{From: "B", To: "C", Payload: "step2"}},
			},
			expectedOrderChains:    [][]string{{"step1", "step2"}},
			expectedParallelGroups: nil,
		},
		{
			name:  "linear_chain_four_messages",
			nodes: []string{"A", "B", "C"},
			phases: [][]send{
				{{From: "A", To: "B", Payload: "a"}},
				{{From: "A", To: "B", Payload: "b"}},
				{{From: "B", To: "C", Payload: "c"}},
				{{From: "B", To: "C", Payload: "d"}},
			},
			expectedOrderChains: [][]string{
				{"a", "b"},
				{"b", "c"},
				{"c", "d"},
			},
			expectedParallelGroups: nil,
		},
		{
			name:  "two_receivers_same_sender_order",
			nodes: []string{"A", "B", "C"},
			phases: [][]send{
				{{From: "A", To: "B", Payload: "toB"}},
				{{From: "A", To: "C", Payload: "toC"}},
			},
			expectedOrderChains:    [][]string{{"toB", "toC"}},
			expectedParallelGroups: nil,
		},
		{
			name:                   "single_node_no_messages",
			nodes:                  []string{"A"},
			phases:                 nil,
			expectedOrderChains:    nil,
			expectedParallelGroups: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sim := hive.NewSimulator(config)
			defer sim.Stop()

			nodeIDs := tt.nodes
			nodes := make(map[string]OrderedNode)
			for _, id := range nodeIDs {
				n := NewReliableOrderedNode(id, nodeIDs)
				nodes[id] = n
				assert.NoError(t, sim.AddNode(n))
			}
			for _, id := range nodeIDs {
				assert.NoError(t, nodes[id].Start(context.Background()))
			}
			sim.Start()

			// Send in phases: wait for each phase to be delivered before sending the next (causal dependency).
			for _, phase := range tt.phases {
				for _, s := range phase {
					assert.NoError(t, nodes[s.From].Send(s.To, s.Payload))
				}
				waitUntilPhaseDelivered(t, nodes, phase, 2*time.Second)
			}

			time.Sleep(100 * time.Millisecond)
			for _, id := range nodeIDs {
				assert.NoError(t, nodes[id].Stop())
			}

			var allSends []send
			for _, phase := range tt.phases {
				allSends = append(allSends, phase...)
			}

			receivers := make(map[string]bool)
			for _, s := range allSends {
				receivers[s.To] = true
			}
			expectedDelivered := make(map[string]int)
			for _, s := range allSends {
				expectedDelivered[s.Payload]++
			}
			var allMsgs []hive.Message
			for id := range receivers {
				delivered := nodes[id].DeliveredMessages()
				for _, m := range delivered {
					p := fmt.Sprint(m.Payload)
					expectedDelivered[p]--
					allMsgs = append(allMsgs, m)
				}
			}
			for payload, count := range expectedDelivered {
				assert.Equal(t, 0, count, "message %s should be delivered exactly once", payload)
			}

			orderer := &Orderer{}
			ordered, parallel := orderer.Order(allMsgs...)
			orderedIdxs := make(map[string]int, len(ordered))
			for i, msg := range ordered {
				orderedIdxs[msg] = i
			}

			for _, orderedPair := range tt.expectedOrderChains {
				assert.Less(t, orderedIdxs[orderedPair[0]], orderedIdxs[orderedPair[1]])
			}

			assert.True(t, verifiyParallelGroups(t, tt.expectedParallelGroups, parallel))
		})
	}
}

func verifiyGroup(t *testing.T, expected []string, actual []string) bool {
	t.Helper()

	if len(expected) != len(actual) {
		return false
	}

	res := make(map[string]int, 0)

	for i := 0; i < len(expected); i++ {
		res[expected[i]] = 1
	}

	for i := 0; i < len(actual); i++ {
		_, exists := res[actual[i]]
		if exists == false {
			return false
		}
	}

	return true
}

func verifiyParallelGroups(t *testing.T, expected [][]string, actual [][]string) bool {
	t.Helper()

	if len(expected) != len(actual) {
		return false
	}

	for i := 0; i < len(expected); i++ {
		found_equal_group := false
		for j := 0; j < len(actual); j++ {
			if verifiyGroup(t, expected[i], actual[j]) {
				found_equal_group = true
				break
			}
		}
		if found_equal_group == false {
			return false
		}
	}

	return true
}

func waitUntilPhaseDelivered(t *testing.T, nodes map[string]OrderedNode, phase []send, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		done := true
		for _, s := range phase {
			delivered := nodes[s.To].DeliveredMessages()
			var found bool
			for _, m := range delivered {
				if fmt.Sprint(m.Payload) == s.Payload {
					found = true
					break
				}
			}
			if !found {
				done = false
				break
			}
		}
		if done {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for phase delivery: %+v", phase)
}
