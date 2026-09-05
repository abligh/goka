package goka

import (
	"github.com/IBM/sarama"
)

// CooperativeCopartitioningStrategy is CopartitioningStrategy taking part in
// incremental cooperative rebalancing (KIP-429) rather than the eager,
// stop-the-world protocol.
//
// It plans exactly as CopartitioningStrategy does — every member is assigned
// the same contiguous partition range across every topic it consumes, which is
// what copartitioning means and what a processor's table lookups depend on —
// and then withholds any partition whose current owner has not yet released it.
// The withheld partitions are assigned in the following generation, once their
// old owner has revoked them, which is the cooperative protocol's two-round
// handover.
//
// Partitions a member keeps are never revoked, so a member that is neither
// gaining nor losing a partition keeps processing throughout a rebalance
// instead of stopping with everyone else.
//
// Requirements, both of which fail loudly rather than silently degrading:
//
//   - The Kafka protocol version must be at least 2.4 (sarama rejects a
//     cooperative protocol below it).
//   - Every member of the group must use a strategy that supports the
//     cooperative protocol. A group is upgraded from eager by rolling out a
//     build that offers both, which this strategy does, and then rolling out
//     one that offers only cooperative.
//
// Like StrictCopartitioningStrategy, this refuses inconsistent topic sets when
// failOnInconsistentTopics is set.
var CooperativeCopartitioningStrategy = &cooperativeCopartitioningStrategy{
	copartitioningStrategy: copartitioningStrategy{},
}

// StrictCooperativeCopartitioningStrategy is CooperativeCopartitioningStrategy
// that fails a rebalance when two members request different topic sets, the way
// StrictCopartitioningStrategy does.
var StrictCooperativeCopartitioningStrategy = &cooperativeCopartitioningStrategy{
	copartitioningStrategy: copartitioningStrategy{failOnInconsistentTopics: true},
}

type cooperativeCopartitioningStrategy struct {
	copartitioningStrategy
}

// Name implements BalanceStrategy. It is deliberately distinct from
// "copartition": the group protocol is chosen by name, so a member offering
// cooperative copartitioning must not be mistaken for one that only speaks the
// eager form.
func (s *cooperativeCopartitioningStrategy) Name() string {
	return "cooperative-copartition"
}

// SupportedProtocols implements sarama's RebalanceProtocolBalanceStrategy.
//
// Both protocols are declared so that a group can be migrated without
// downtime: while any member still offers only the eager protocol, sarama
// selects eager for the whole group, and the group switches to cooperative by
// itself once every member offers it.
func (s *cooperativeCopartitioningStrategy) SupportedProtocols() []sarama.RebalanceProtocol {
	return []sarama.RebalanceProtocol{
		sarama.RebalanceProtocolCooperative,
		sarama.RebalanceProtocolEager,
	}
}

// Plan implements BalanceStrategy: the copartitioned plan, minus whatever is
// still held by a different member.
func (s *cooperativeCopartitioningStrategy) Plan(members map[string]sarama.ConsumerGroupMemberMetadata, topics map[string][]int32) (sarama.BalanceStrategyPlan, error) {
	plan, err := s.copartitioningStrategy.Plan(members, topics)
	if err != nil {
		return nil, err
	}
	withholdOwnedPartitions(plan, members)
	return plan, nil
}

// withholdOwnedPartitions removes from the plan every partition that a
// *different* member reports still owning, so no partition is assigned to two
// members at once. Sarama's own cooperative-sticky assignor does the same thing
// with an unexported helper; this is that behavior, spelled out.
//
// A withheld partition is not lost: its current owner sees it missing from its
// own assignment, revokes it, and the rebalance that follows assigns it to the
// member the plan intended.
//
// Withholding is decided per partition *number*, across every topic, not per
// topic independently — which is the whole difficulty. A member may still hold
// partition 2 of the loop while having revoked partition 2 of the input, and
// assigning it partition 2 for some topics and not others would hand the new
// owner a partition set that is not copartitioned. Since a processor may only
// look up its own table for the key it is handling, that is precisely the state
// this strategy exists to make impossible: a partition number is withheld from
// a member for all of its topics, or for none.
func withholdOwnedPartitions(plan sarama.BalanceStrategyPlan, members map[string]sarama.ConsumerGroupMemberMetadata) {
	// owner[topic][partition] is the member that reported still owning it.
	owner := make(map[string]map[int32]string)
	for memberID, meta := range members {
		for _, owned := range meta.OwnedPartitions {
			if owner[owned.Topic] == nil {
				owner[owned.Topic] = make(map[int32]string)
			}
			for _, partition := range owned.Partitions {
				owner[owned.Topic][partition] = memberID
			}
		}
	}

	for memberID, topicPartitions := range plan {
		// A partition number is withheld from this member if any topic's copy
		// of it is still held by somebody else.
		withheld := make(map[int32]bool)
		for topic, partitions := range topicPartitions {
			for _, partition := range partitions {
				if current, held := owner[topic][partition]; held && current != memberID {
					withheld[partition] = true
				}
			}
		}
		if len(withheld) == 0 {
			continue
		}

		for topic, partitions := range topicPartitions {
			keep := make([]int32, 0, len(partitions))
			for _, partition := range partitions {
				if withheld[partition] {
					continue
				}
				keep = append(keep, partition)
			}
			if len(keep) == 0 {
				delete(topicPartitions, topic)
				continue
			}
			topicPartitions[topic] = keep
		}
		if len(topicPartitions) == 0 {
			delete(plan, memberID)
		}
	}
}
