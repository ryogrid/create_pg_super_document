# Barrier

## Location
[src/include/storage/barrier.h:25-35](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/barrier.h#L25-L35)

## Overview
The Barrier struct is a synchronization primitive used in PostgreSQL for coordinating parallel processes, ensuring that cooperating processes reach specific synchronization points before proceeding to the next phase of computation.

## Definition

```c
typedef struct Barrier
{
	slock_t		mutex;
	int			phase;			/* phase counter */
	int			participants;	/* the number of participants attached */
	int			arrived;		/* the number of participants that have
								 * arrived */
	int			elected;		/* highest phase elected */
	bool		static_party;	/* used only for assertions */
	ConditionVariable condition_variable;
} Barrier;
```
## Detailed Description
The Barrier struct implements a synchronization mechanism that allows multiple parallel processes to coordinate their execution. It supports both static barriers (with a fixed number of participants known at initialization) and dynamic barriers (where participants can join and leave at runtime).

The barrier operates using a phase-based approach where all participants must reach a synchronization point before any can proceed to the next phase. This is essential for parallel algorithms that have distinct phases where the output of each phase is required before the next phase can begin.

Static barriers behave similarly to POSIX's pthread_barrier_t, while dynamic barriers behave similarly to Java's java.util.concurrent.Phaser. The barrier ensures that when BarrierArriveAndWait() is called, the calling process will block until all other attached participants have also arrived at the barrier.

## Parameters / Member Variables
- `mutex`: A spinlock that protects the barrier's internal state from concurrent access by multiple processes
- `phase`: A counter that tracks the current phase number of the barrier, incremented each time all participants arrive
- `participants`: The total number of processes currently attached to this barrier
- `arrived`: The count of participants that have arrived at the current synchronization point but are still waiting
- `elected`: The highest phase number for which a participant has been elected to perform serial work
- `static_party`: A boolean flag used for assertions to distinguish between static and dynamic barrier usage patterns
- `condition_variable`: Used to efficiently wake up waiting processes when all participants have arrived
## Dependencies
- Functions called/Symbols referenced:
  - [slock_t](../s/slock_t.md) (spinlock type for thread-safe access)
  - ConditionVariable (for process synchronization)
- Called from (representative examples):
  - [MultiExecParallelHash](../M/MultiExecParallelHash.md) (parallel hash table execution)
  - ExecHashTableCreate (hash table creation in parallel execution)
  - [ExecHashJoinImpl](../E/ExecHashJoinImpl.md) (parallel hash join implementation)
  - ParallelHashJoinState (state management for parallel hash joins)

## Notes and Other Information
- The barrier is primarily used in PostgreSQL's parallel query execution, particularly for hash joins and hash table operations
- One participant is arbitrarily chosen to return true from BarrierArriveAndWait(), allowing for leader election in parallel algorithms
- The implementation supports both cooperative and preemptive detachment of participants
- For static barriers, participants should be implicitly attached at initialization; for dynamic barriers, explicit attachment/detachment is required
- The phase counter enables late-joining participants in dynamic barriers to synchronize with the current state of computation
- Used extensively in parallel hash join operations to coordinate different phases like building hash tables and probing