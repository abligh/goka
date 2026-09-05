package systemtest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/abligh/goka"
	"github.com/abligh/goka/codec"
	"github.com/abligh/goka/multierr"
	"github.com/stretchr/testify/require"
)

// TestCooperativeRebalance demonstrates what the cooperative protocol buys: a
// processor that is already running keeps processing while another joins the
// group, instead of stopping with everyone else while the group reassigns.
//
// It is the difference the eager protocol cannot express. Under eager, every
// member gives up every partition on every membership change, so the whole
// group pauses for as long as the slowest member takes to stop, hand back and
// recover — which for a stateful processor includes recovering its table. Under
// cooperative, a member that is neither gaining nor losing a partition never
// stops at all, and one that is losing a partition stops only that partition.
//
// The assertion is therefore about a gap in processing, not about the final
// assignment: the first processor must not stop for longer than
// maxAcceptableGap while the second joins.
//
// Requires a broker (GOKA_SYSTEMTEST) and Kafka 2.4+, which is what sarama
// requires before it will negotiate the cooperative protocol at all.
func TestCooperativeRebalance(t *testing.T) {
	brokers := initSystemTest(t)

	var (
		group       = goka.Group(fmt.Sprintf("goka-systemtest-cooperative-%d", time.Now().Unix()))
		inputStream = string(group) + "-input"

		// Generous: this is testing that processing does not stop for a
		// rebalance, not that it is fast. An eager rebalance of a recovered
		// table takes seconds; a cooperative one should not interrupt the
		// member that keeps its partitions at all.
		maxAcceptableGap = 3 * time.Second
	)

	cfg := goka.DefaultConfig()
	// Both are required, and neither implies the other: the version is what
	// lets sarama negotiate the protocol, and the strategy is what speaks it
	// while keeping the group copartitioned.
	cfg.Version = sarama.V2_4_0_0
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		goka.CooperativeCopartitioningStrategy,
	}

	tmc := goka.NewTopicManagerConfig()
	tmc.Table.Replication = 1
	tmc.Stream.Replication = 1
	tm, err := goka.TopicManagerBuilderWithConfig(cfg, tmc)(brokers)
	require.NoError(t, err)
	require.NoError(t, tm.EnsureStreamExists(inputStream, 8))

	// A steady stream of input, so a pause in processing is visible.
	em, err := goka.NewEmitter(brokers, goka.Stream(inputStream), new(codec.String))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	emitters, _ := multierr.NewErrGroup(ctx)
	emitters.Go(func() error {
		defer em.Finish()
		for i := 0; ; i++ {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(20 * time.Millisecond):
				em.EmitSync(fmt.Sprintf("key-%d", i%64), "value")
			}
		}
	})

	// lastSeen records when the first processor last processed anything, so
	// the test can measure how long it stopped for.
	var (
		mu       sync.Mutex
		lastSeen = time.Now()
		maxGap   time.Duration
	)
	observe := func() {
		mu.Lock()
		defer mu.Unlock()
		if gap := time.Since(lastSeen); gap > maxGap {
			maxGap = gap
		}
		lastSeen = time.Now()
	}

	newProcessor := func(storages *storageTracker, cb goka.ProcessCallback) *goka.Processor {
		proc, err := goka.NewProcessor(brokers,
			goka.DefineGroup(group,
				goka.Input(goka.Stream(inputStream), new(codec.String), cb),
				goka.Persist(new(codec.String)),
			),
			goka.WithCooperativeRebalance(),
			goka.WithConsumerGroupBuilder(goka.ConsumerGroupBuilderWithConfig(cfg)),
			goka.WithProducerBuilder(goka.ProducerBuilderWithConfig(cfg)),
			goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithConfig(cfg, tmc)),
			goka.WithStorageBuilder(storages.Build),
		)
		require.NoError(t, err)
		return proc
	}

	first := newProcessor(newStorageTracker(), func(ctx goka.Context, msg any) {
		observe()
		ctx.SetValue(msg)
	})
	second := newProcessor(newStorageTracker(), func(ctx goka.Context, msg any) {
		ctx.SetValue(msg)
	})

	procs, procCtx := multierr.NewErrGroup(ctx)
	procs.Go(func() error { return first.Run(procCtx) })
	require.NoError(t, first.WaitForReadyContext(procCtx))

	// Let it settle into a steady state before anything joins.
	time.Sleep(3 * time.Second)
	mu.Lock()
	maxGap = 0
	mu.Unlock()

	// The event under test: a second member joins, which under the eager
	// protocol stops the first one dead.
	procs.Go(func() error { return second.Run(procCtx) })
	require.NoError(t, second.WaitForReadyContext(procCtx))

	// Give the handover its second round: withheld partitions are assigned in
	// the generation after their old owner revokes them.
	time.Sleep(10 * time.Second)

	mu.Lock()
	observed := maxGap
	mu.Unlock()

	require.Less(t, observed, maxAcceptableGap,
		"the first processor stopped for %s while the second joined; under the cooperative "+
			"protocol a member that keeps its partitions should not stop at all", observed)

	// Both members must end up doing work, or "nothing stopped" would be
	// trivially true because nothing moved.
	require.NotEmpty(t, first.Stats().Group, "the first processor ended up with no partitions")
	require.NotEmpty(t, second.Stats().Group, "the second processor never gained a partition, so nothing actually moved")

	cancel()
	require.NoError(t, procs.Wait().ErrorOrNil())
}
