# FixedParallelState

## Location
[src/backend/access/transam/parallel.c:81-106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/parallel.c#L81-L106)

## Overview
FixedParallelState is a structure that holds fixed-size state information shared between the leader process and parallel worker processes in PostgreSQL's parallel query execution system.

## Definition

```c
typedef struct FixedParallelState
{
	/* Fixed-size state that workers must restore. */
	Oid			database_id;
	Oid			authenticated_user_id;
	Oid			session_user_id;
	Oid			outer_user_id;
	Oid			current_user_id;
	Oid			temp_namespace_id;
	Oid			temp_toast_namespace_id;
	int			sec_context;
	bool		session_user_is_superuser;
	bool		role_is_superuser;
	PGPROC	   *parallel_leader_pgproc;
	pid_t		parallel_leader_pid;
	ProcNumber	parallel_leader_proc_number;
	TimestampTz xact_ts;
	TimestampTz stmt_ts;
	SerializableXactHandle serializable_xact_handle;

	/* Mutex protects remaining fields. */
	slock_t		mutex;

	/* Maximum XactLastRecEnd of any worker. */
	XLogRecPtr	last_xlog_end;
} FixedParallelState;
```
## Detailed Description
FixedParallelState stores essential session and transaction state that parallel workers need to restore to maintain consistency with the leader process. This structure is allocated in shared memory as part of the dynamic shared memory (DSM) segment created for parallel operations. The structure contains two main categories of information: session state that workers must restore to match the leader's context, and coordination data protected by a mutex for synchronization between the leader and workers.

The structure ensures that parallel workers operate with the same database context, user privileges, security settings, and transaction state as the leader process. It also provides mechanisms for tracking transaction log positions and maintaining proper isolation between parallel processes.

## Parameters / Member Variables
- : OID of the database being accessed by the parallel operation
- : OID of the user that was authenticated for this session
- : OID of the session user (may differ from authenticated user due to SET SESSION AUTHORIZATION)
- : OID of the user in the outer security context
- : OID of the current effective user
- : OID of the temporary schema namespace for this session
- : OID of the temporary TOAST schema namespace
- : Security context flags indicating special privileges or restrictions
- : Whether the session user has superuser privileges
- : Whether the current role has superuser privileges
- : Pointer to the leader process's PGPROC structure
- : Process ID of the parallel leader
- : Process number of the parallel leader in the process array
- : Transaction start timestamp
- : Statement start timestamp
- : Handle for serializable transaction state sharing
- : Spinlock protecting the remaining fields from concurrent access
- : Maximum XactLastRecEnd value across all worker processes

## Dependencies
- Functions called/Symbols referenced:
  - [PGPROC](../P/PGPROC.md) (process control block structure)
  - pid_t (process ID type)
  - ProcNumber (process number type)
  - SerializableXactHandle (serializable transaction handle type)
  - [slock_t](../s/slock_t.md) (spinlock type)
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md) (initializes the structure in shared memory)
  - [ReinitializeParallelDSM](../R/ReinitializeParallelDSM.md) (reinitializes for reused parallel contexts)
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) (workers read state from this structure)
  - WaitForParallelWorkersToFinish (leader accesses coordination data)
  - [ParallelWorkerReportLastRecEnd](../P/ParallelWorkerReportLastRecEnd.md) (workers update last_xlog_end field)

## Notes and Other Information
The structure is designed to be fixed-size to simplify memory management in the shared memory segment. Variable-length state information is stored separately in the DSM segment and referenced by keys in the table of contents. The mutex field specifically protects only the last_xlog_end field, which is updated by workers to report their transaction log positions back to the leader. This coordination is important for ensuring proper WAL synchronization in parallel operations.

The structure is critical for maintaining ACID properties and security isolation in parallel query execution, ensuring that all workers operate with identical session context as the leader process.