# AutoVacuumWorkItem

## Location
[src/backend/postmaster/autovacuum.c:258-266](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/autovacuum.c#L258-L266)

## Overview
AutoVacuumWorkItem is a structure used in PostgreSQL's autovacuum system to represent specific work requests that can be submitted by other processes for autovacuum workers to perform.

## Definition

```c
typedef struct AutoVacuumWorkItem
{
	AutoVacuumWorkItemType avw_type;
	bool		avw_used;		/* below data is valid */
	bool		avw_active;		/* being processed */
	Oid			avw_database;
	Oid			avw_relation;
	BlockNumber avw_blockNumber;
} AutoVacuumWorkItem;
```
## Detailed Description
AutoVacuumWorkItem represents a work request that external processes can submit to the autovacuum system for specialized maintenance tasks. This structure is stored in the autovacuum shared memory work item array (AutoVacuumShmem->av_workItems) and allows coordination between regular PostgreSQL operations and the autovacuum workers.

The primary use case is for operations that need specialized maintenance work to be performed asynchronously by autovacuum workers rather than blocking the requesting process. Currently, this includes BRIN index summarization tasks that can be offloaded to autovacuum workers.

The structure includes state management fields to track whether the item is in use and currently being processed, along with identifying information about the specific database, relation, and block number that needs attention.

## Parameters / Member Variables
- : AutoVacuumWorkItemType enum indicating the type of work requested (currently supports AVW_BRINSummarizeRange)
- : Boolean flag indicating whether the work item data is valid and the slot is occupied
- : Boolean flag indicating whether the work item is currently being processed by a worker
- : OID of the database containing the object that needs work
- : OID of the specific relation (table/index) that needs attention
- : BlockNumber specifying the particular block range for the work operation

## Dependencies
- Functions called/Symbols referenced:
  - [AutoVacuumWorkItemType](AutoVacuumWorkItemType.md) (enum for work item types)
  - Oid (PostgreSQL object identifier type)
  - BlockNumber (block number type for storage management)

- Called from (representative examples):
  - [do_autovacuum](../d/do_autovacuum.md) (src/backend/postmaster/autovacuum.c:2518)
  - [perform_work_item](../p/perform_work_item.md) (src/backend/postmaster/autovacuum.c:2588)
  - [autovac_report_workitem](../a/autovac_report_workitem.md) (src/backend/postmaster/autovacuum.c:3193)
  - [AutoVacuumRequestWork](AutoVacuumRequestWork.md) (src/backend/postmaster/autovacuum.c:3258)
  - [AutoVacuumShmemInit](AutoVacuumShmemInit.md) (src/backend/postmaster/autovacuum.c:3340)

## Notes and Other Information
- The work item array is stored in shared memory as part of the AutoVacuumShmem structure
- Most fields are protected by AutovacuumLock, except when an item is marked 'active', other processes must not modify the work-identifying members
- This mechanism allows for asynchronous execution of maintenance tasks without blocking user operations
- Currently used primarily for BRIN index summarization operations that can be deferred to background autovacuum workers
- The work item system provides a way to queue maintenance operations that don't need immediate execution
- Work items are processed by autovacuum workers as part of their regular maintenance cycles
- The structure enables fine-grained control over which specific blocks or ranges need attention rather than processing entire relations