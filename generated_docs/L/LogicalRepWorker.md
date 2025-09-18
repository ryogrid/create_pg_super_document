# LogicalRepWorker

## Location
src/include/replication/worker_internal.h: 37 - 95

## Overview
LogicalRepWorker is a core data structure that represents a logical replication worker process in PostgreSQL's logical replication system, managing both apply workers and table synchronization workers.

## Definition


## Detailed Description
LogicalRepWorker is a shared memory structure that tracks the state and configuration of individual logical replication worker processes. It serves as the central control structure for managing both apply workers (which apply changes from the publisher) and table synchronization workers (which perform initial data synchronization for new tables). The structure supports parallel apply functionality where multiple workers can process changes concurrently, with leader-follower relationships tracked through the leader_pid field.

## Parameters / Member Variables
- : The type of logical replication worker (apply, sync, parallel apply, etc.)
- : Timestamp when this worker process was started
- : Boolean flag indicating whether this worker slot is currently active
- : Counter incremented each time the slot is reassigned to prevent stale references
- : Pointer to the process array entry for this worker, NULL when not running
- : Object ID of the target database the worker connects to
- : Object ID of the user account used for replication connections
- : Object ID of the subscription this worker serves
- : Object ID of the relation being synchronized (for table sync workers)
- : Current synchronization state of the relation
- : LSN position associated with the relation state
- : Spinlock protecting relation state information
- : File set for managing streaming transaction data files
- : Process ID of the leader worker for parallel apply workers
- : Flag indicating if this worker supports parallel processing
- : Last LSN position processed by this worker
- : Timestamp of last message sent by this worker
- : Timestamp of last message received by this worker
- : LSN position of last reply sent to publisher
- : Timestamp of last reply sent to publisher

## Dependencies
- Functions called/Symbols referenced:
  - LogicalRepWorkerType
  - PGPROC
  - slock_t
  - FileSet
  - pid_t
- Called from (representative examples):
  - logicalrep_worker_find
  - logicalrep_worker_launch
  - logicalrep_worker_stop_internal
  - logicalrep_worker_detach
  - ApplyLauncherShmemInit

## Notes and Other Information
This structure is allocated in shared memory and is the primary means of communication and coordination between the logical replication launcher process and individual worker processes. The generation counter mechanism helps prevent race conditions when workers are rapidly started and stopped. The parallel apply functionality allows for improved performance by enabling concurrent processing of changes from different transactions, with proper coordination through the leader_pid mechanism.