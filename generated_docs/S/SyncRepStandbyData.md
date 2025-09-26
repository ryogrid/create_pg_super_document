# SyncRepStandbyData

## Location
[src/include/replication/syncrep.h:42-54](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/replication/syncrep.h#L42-L54)

## Overview
SyncRepStandbyData is a struct that represents data about a candidate synchronous standby server in PostgreSQL's synchronous replication system. It is used to collect and compare information about standby servers when determining which standbys are eligible for synchronous replication.

## Definition

```c
typedef struct SyncRepStandbyData
{
	/* Copies of relevant fields from WalSnd shared-memory struct */
	pid_t		pid;
	XLogRecPtr	write;
	XLogRecPtr	flush;
	XLogRecPtr	apply;
	int			sync_standby_priority;
	/* Index of this walsender in the WalSnd shared-memory array */
	int			walsnd_index;
	/* This flag indicates whether this struct is about our own process */
	bool		is_me;
} SyncRepStandbyData;
```
## Detailed Description
SyncRepStandbyData is a data structure that encapsulates information about a standby server that is a candidate for synchronous replication. This struct is returned by SyncRepGetCandidateStandbys() as an array, with one entry per candidate synchronous walsender. The struct copies relevant fields from the WalSnd shared-memory structure to provide a snapshot of the standby's replication status at a specific point in time. This allows the synchronous replication subsystem to make decisions about which standbys to wait for based on their current replication positions and priorities.

## Parameters / Member Variables
- : Process ID of the walsender process serving this standby
- : XLog position up to which the standby has written WAL data to disk
- : XLog position up to which the standby has flushed WAL data to disk
- : XLog position up to which the standby has applied WAL data
- : Priority level of this standby for synchronous replication (higher numbers indicate higher priority)
- : Index of the corresponding walsender in the WalSnd shared-memory array
- : Boolean flag indicating whether this struct represents the current process (used for self-identification)

## Dependencies
- Functions called/Symbols referenced:
  - pid_t
  - XLogRecPtr
- Called from (representative examples):
  - SyncStandbysDefined
  - SyncRepGetSyncRecPtr
  - SyncRepGetOldestSyncRecPtr
  - SyncRepGetNthLatestSyncRecPtr
  - SyncRepGetCandidateStandbys
  - standby_priority_comparator

## Notes and Other Information
- This struct is used internally by the synchronous replication subsystem to manage and compare standby servers
- The struct provides a point-in-time snapshot of standby status, copied from shared memory to avoid locking issues
- The priority field is used to determine which standbys should be considered for synchronous replication when multiple standbys are available
- The XLogRecPtr fields (write, flush, apply) represent different stages of WAL processing on the standby, allowing fine-grained control over synchronization requirements