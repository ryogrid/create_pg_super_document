# ParallelHashJoinState

## Location
[src/include/executor/hashjoin.h:246-266](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/hashjoin.h#L246-L266)

## Overview
ParallelHashJoinState is a shared structure stored in DSM (Dynamic Shared Memory) that coordinates the execution of parallel hash joins across multiple worker processes, managing batches, buckets, growth operations, and synchronization.

## Definition
```c
typedef struct ParallelHashJoinState
{
    dsa_pointer batches;                /* array of ParallelHashJoinBatch */
    dsa_pointer old_batches;            /* previous generation during repartition */
    int         nbatch;                 /* number of batches now */
    int         old_nbatch;             /* previous number of batches */
    int         nbuckets;               /* number of buckets */
    ParallelHashGrowth growth;          /* control batch/bucket growth */
    dsa_pointer chunk_work_queue;       /* chunk work queue */
    int         nparticipants;
    size_t      space_allowed;
    size_t      total_tuples;           /* total number of inner tuples */
    LWLock      lock;                   /* lock protecting the above */
    
    Barrier     build_barrier;          /* synchronization for the build phases */
    Barrier     grow_batches_barrier;
    Barrier     grow_buckets_barrier;
    pg_atomic_uint32 distributor;       /* counter for load balancing */
    
    SharedFileSet fileset;              /* space for shared temporary files */
} ParallelHashJoinState;
```

## Detailed Description
ParallelHashJoinState serves as the central coordination point for parallel hash join operations. It resides in a DSM segment accessible to all participating worker processes and contains all the shared state needed to coordinate the complex multi-phase parallel hash join algorithm.

The structure manages the dynamic aspects of parallel hash joins, including the ability to grow the number of batches (when memory pressure is high) or buckets (when load factors become unbalanced). It coordinates these growth operations across all workers using barriers to ensure consistency.

The state includes both current and previous batch information to support repartitioning operations, where data is redistributed across a different number of batches. Work distribution is managed through a chunk work queue and an atomic distributor counter for load balancing.

Multiple synchronization barriers coordinate different phases of the parallel hash join: building the hash table, growing batches when memory is constrained, and growing buckets when load distribution is uneven.

## Parameters / Member Variables
- `batches`: DSA pointer to an array of ParallelHashJoinBatch structures representing the current generation of batches
- `old_batches`: DSA pointer to the previous generation of batches during repartitioning operations
- `nbatch`: Current number of batches being used for the hash join
- `old_nbatch`: Previous number of batches, maintained during repartitioning to access old data
- `nbuckets`: Number of hash buckets within each batch
- `growth`: Enumerated value controlling whether batch or bucket growth should occur
- `chunk_work_queue`: DSA pointer to a queue of work chunks for load balancing across workers
- `nparticipants`: Number of worker processes participating in the parallel hash join
- `space_allowed`: Maximum memory space allowed for the hash table
- `total_tuples`: Total count of tuples from the inner relation across all workers
- `lock`: LWLock protecting the shared state fields above
- `build_barrier`: Barrier synchronizing the hash table build phases across all workers
- `grow_batches_barrier`: Barrier coordinating batch growth operations
- `grow_buckets_barrier`: Barrier coordinating bucket growth operations  
- `distributor`: Atomic counter used for load balancing work distribution among workers
- `fileset`: Shared file set for managing temporary files needed for batches that spill to disk

## Dependencies
- Functions called/Symbols referenced:
  - dsa_pointer
  - ParallelHashGrowth
  - LWLock
  - Barrier
  - pg_atomic_uint32
  - SharedFileSet
- Called from (representative examples):
  - MultiExecParallelHash
  - ExecHashTableCreate
  - ExecParallelHashIncreaseNumBatches
  - ExecParallelHashMergeCounters
  - ExecParallelHashJoinSetUpBatches
  - ExecHashJoinInitializeDSM
  - HashJoinTableData (as member)
  - HashState (as member)

## Notes and Other Information
This structure is the cornerstone of PostgreSQL's parallel hash join implementation, enabling multiple worker processes to cooperatively build and probe hash tables. The design handles the complex synchronization requirements of dynamic hash table growth while maintaining load balance across workers. The structure must handle memory management across multiple processes, coordinate file I/O for batches that exceed memory limits, and ensure consistent state during repartitioning operations. The barrier-based synchronization ensures that all workers complete each phase before proceeding to the next, maintaining data consistency throughout the parallel execution.