package goka

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
)

// meta builds the member metadata a member sends in its JoinGroup request:
// the topics it wants, and the partitions it currently owns.
func meta(topics []string, owned map[string][]int32) sarama.ConsumerGroupMemberMetadata {
	m := sarama.ConsumerGroupMemberMetadata{Topics: topics}
	for topic, partitions := range owned {
		m.OwnedPartitions = append(m.OwnedPartitions, &sarama.OwnedPartition{
			Topic:      topic,
			Partitions: partitions,
		})
	}
	return m
}

// TestCooperativeCopartitioning_DeclaresBothProtocols pins that a group can be
// migrated from eager without downtime: while any member still offers only the
// eager protocol, sarama selects eager for the whole group.
func TestCooperativeCopartitioning_DeclaresBothProtocols(t *testing.T) {
	require.Equal(t,
		[]sarama.RebalanceProtocol{sarama.RebalanceProtocolCooperative, sarama.RebalanceProtocolEager},
		CooperativeCopartitioningStrategy.SupportedProtocols())

	// The name must differ from the eager strategy's: the group protocol is
	// selected by name, so a cooperative member must not be taken for an
	// eager one.
	require.NotEqual(t, CopartitioningStrategy.Name(), CooperativeCopartitioningStrategy.Name())
}

// TestCooperativeCopartitioning_PlansTheSameAsEagerWhenNothingIsOwned pins that
// this is the copartitioning strategy and not a new assignment scheme: on a
// first join, where no member owns anything yet, it must plan exactly what the
// eager strategy plans.
func TestCooperativeCopartitioning_PlansTheSameAsEagerWhenNothingIsOwned(t *testing.T) {
	topics := map[string][]int32{"input": {0, 1, 2, 3}, "table": {0, 1, 2, 3}}
	members := map[string]sarama.ConsumerGroupMemberMetadata{
		"a": meta([]string{"input", "table"}, nil),
		"b": meta([]string{"input", "table"}, nil),
	}

	eager, err := CopartitioningStrategy.Plan(members, topics)
	require.NoError(t, err)
	cooperative, err := CooperativeCopartitioningStrategy.Plan(members, topics)
	require.NoError(t, err)

	require.Equal(t, eager, cooperative)
}

// TestCooperativeCopartitioning_WithholdsPartitionsOwnedElsewhere is the
// protocol's whole point: a partition moving from one member to another is not
// assigned to its new owner until its old owner has revoked it, so it is never
// owned by two members at once.
func TestCooperativeCopartitioning_WithholdsPartitionsOwnedElsewhere(t *testing.T) {
	topics := map[string][]int32{"input": {0, 1, 2, 3}}

	// "a" holds everything; "b" is joining, so half should move to it.
	members := map[string]sarama.ConsumerGroupMemberMetadata{
		"a": meta([]string{"input"}, map[string][]int32{"input": {0, 1, 2, 3}}),
		"b": meta([]string{"input"}, nil),
	}

	plan, err := CooperativeCopartitioningStrategy.Plan(members, topics)
	require.NoError(t, err)

	// "a" keeps the half it is meant to keep and is not asked to give it up.
	require.Equal(t, []int32{0, 1}, plan["a"]["input"])

	// "b" is assigned nothing yet: partitions 2 and 3 are still owned by "a",
	// which has not seen the new assignment. Assigning them now would make
	// them owned twice.
	require.NotContains(t, plan, "b")

	// The eager strategy, by contrast, hands them over immediately — which is
	// safe only because every member has already stopped everything.
	eager, err := CopartitioningStrategy.Plan(members, topics)
	require.NoError(t, err)
	require.Equal(t, []int32{2, 3}, eager["b"]["input"])
}

// TestCooperativeCopartitioning_AssignsOnceRevoked pins the second round of the
// handover: when the old owner has revoked the partitions, they are assigned.
func TestCooperativeCopartitioning_AssignsOnceRevoked(t *testing.T) {
	topics := map[string][]int32{"input": {0, 1, 2, 3}}
	members := map[string]sarama.ConsumerGroupMemberMetadata{
		// "a" has revoked 2 and 3, and now reports owning only 0 and 1.
		"a": meta([]string{"input"}, map[string][]int32{"input": {0, 1}}),
		"b": meta([]string{"input"}, nil),
	}

	plan, err := CooperativeCopartitioningStrategy.Plan(members, topics)
	require.NoError(t, err)
	require.Equal(t, []int32{0, 1}, plan["a"]["input"])
	require.Equal(t, []int32{2, 3}, plan["b"]["input"])
}

// TestCooperativeCopartitioning_KeepsCopartitioning is the invariant the whole
// strategy exists for: whatever is withheld, a member holds the *same*
// partition numbers across every topic it consumes, because a processor can
// only look up its own table for the key it is handling.
func TestCooperativeCopartitioning_KeepsCopartitioning(t *testing.T) {
	topics := map[string][]int32{"input": {0, 1, 2, 3}, "loop": {0, 1, 2, 3}, "table": {0, 1, 2, 3}}
	all := []string{"input", "loop", "table"}

	for name, members := range map[string]map[string]sarama.ConsumerGroupMemberMetadata{
		"first join": {
			"a": meta(all, nil),
			"b": meta(all, nil),
		},
		"mid-handover": {
			"a": meta(all, map[string][]int32{"input": {0, 1, 2, 3}, "loop": {0, 1, 2, 3}, "table": {0, 1, 2, 3}}),
			"b": meta(all, nil),
		},
		"partially revoked": {
			"a": meta(all, map[string][]int32{"input": {0, 1}, "loop": {0, 1, 2}, "table": {0, 1}}),
			"b": meta(all, nil),
		},
	} {
		t.Run(name, func(t *testing.T) {
			plan, err := CooperativeCopartitioningStrategy.Plan(members, topics)
			require.NoError(t, err)

			for member, assigned := range plan {
				var first []int32
				for _, topic := range all {
					partitions, ok := assigned[topic]
					if !ok {
						continue
					}
					if first == nil {
						first = partitions
						continue
					}
					require.Equal(t, first, partitions,
						"member %q was assigned different partitions for different topics, which breaks copartitioning", member)
				}
			}
		})
	}
}

// TestCooperativeCopartitioning_NeverAssignsAPartitionTwice pins the safety
// property directly, across the same scenarios.
func TestCooperativeCopartitioning_NeverAssignsAPartitionTwice(t *testing.T) {
	topics := map[string][]int32{"input": {0, 1, 2, 3, 4, 5}}
	members := map[string]sarama.ConsumerGroupMemberMetadata{
		"a": meta([]string{"input"}, map[string][]int32{"input": {0, 1, 2}}),
		"b": meta([]string{"input"}, map[string][]int32{"input": {3, 4, 5}}),
		"c": meta([]string{"input"}, nil),
	}

	plan, err := CooperativeCopartitioningStrategy.Plan(members, topics)
	require.NoError(t, err)

	seen := map[int32]string{}
	for member, assigned := range plan {
		for _, partition := range assigned["input"] {
			if other, dup := seen[partition]; dup {
				t.Fatalf("partition %d assigned to both %q and %q", partition, other, member)
			}
			seen[partition] = member
		}
	}
}
