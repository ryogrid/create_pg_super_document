# ComputeXidHorizonsResult

## Location
[src/backend/storage/ipc/procarray.c:179-244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L179-L244)

## Overview
ComputeXidHorizonsResult is a structure that contains comprehensive transaction visibility horizon information computed by ComputeXidHorizons(), providing different XID boundaries for various types of relations and operations.

## Definition

```c
typedef struct ComputeXidHorizonsResult
{
	/*
	 * The value of TransamVariables->latestCompletedXid when
	 * ComputeXidHorizons() held ProcArrayLock.
	 */
	FullTransactionId latest_completed;

	/*
	 * The same for procArray->replication_slot_xmin and.
	 * procArray->replication_slot_catalog_xmin.
	 */
	TransactionId slot_xmin;
	TransactionId slot_catalog_xmin;

	/*
	 * Oldest xid that any backend might still consider running. This needs to
	 * include processes running VACUUM, in contrast to the normal visibility
	 * cutoffs, as vacuum needs to be able to perform pg_subtrans lookups when
	 * determining visibility, but doesn't care about rows above its xmin to
	 * be removed.
	 *
	 * This likely should only be needed to determine whether pg_subtrans can
	 * be truncated. It currently includes the effects of replication slots,
	 * for historical reasons. But that could likely be changed.
	 */
	TransactionId oldest_considered_running;

	/*
	 * Oldest xid for which deleted tuples need to be retained in shared
	 * tables.
	 *
	 * This includes the effects of replication slots. If that's not desired,
	 * look at shared_oldest_nonremovable_raw;
	 */
	TransactionId shared_oldest_nonremovable;

	/*
	 * Oldest xid that may be necessary to retain in shared tables. This is
	 * the same as shared_oldest_nonremovable, except that is not affected by
	 * replication slot's catalog_xmin.
	 *
	 * This is mainly useful to be able to send the catalog_xmin to upstream
	 * streaming replication servers via hot_standby_feedback, so they can
	 * apply the limit only when accessing catalog tables.
	 */
	TransactionId shared_oldest_nonremovable_raw;

	/*
	 * Oldest xid for which deleted tuples need to be retained in non-shared
	 * catalog tables.
	 */
	TransactionId catalog_oldest_nonremovable;

	/*
	 * Oldest xid for which deleted tuples need to be retained in normal user
	 * defined tables.
	 */
	TransactionId data_oldest_nonremovable;

	/*
	 * Oldest xid for which deleted tuples need to be retained in this
	 * session's temporary tables.
	 */
	TransactionId temp_oldest_nonremovable;
} ComputeXidHorizonsResult;
```
## Detailed Description
ComputeXidHorizonsResult encapsulates the comprehensive set of transaction visibility horizons computed by the ComputeXidHorizons() function. This structure is crucial for determining which deleted tuples can be safely removed during vacuum and other cleanup operations while respecting MVCC visibility rules for different classes of relations.

The structure provides fine-grained control over tuple retention by distinguishing between different types of tables (shared, catalog, data, temporary) and different types of operations (replication, visibility checking, subtransaction tracking). Each horizon represents the oldest transaction ID that might still need to see tuples deleted by newer transactions in the corresponding context.

The differentiation between raw and non-raw horizons allows the system to handle replication requirements separately from local visibility requirements, which is essential for streaming replication and logical replication scenarios.

## Parameters / Member Variables
- `latest_completed`: The most recent transaction ID that was completed when ComputeXidHorizons() acquired ProcArrayLock, providing a reference point for the computation
- `slot_xmin`: The oldest transaction ID that any replication slot still needs for data visibility
- `slot_catalog_xmin`: The oldest catalog transaction ID that any replication slot still needs for DDL change visibility
- `oldest_considered_running`: The oldest XID that any backend (including VACUUM) might still consider running, primarily used for pg_subtrans truncation decisions
- `shared_oldest_nonremovable`: The oldest XID for which deleted tuples must be retained in shared catalog tables, including replication slot effects
- `shared_oldest_nonremovable_raw`: Similar to shared_oldest_nonremovable but excluding replication slot catalog_xmin effects, used for hot_standby_feedback
- `catalog_oldest_nonremovable`: The oldest XID for which deleted tuples must be retained in database-specific catalog tables
- `data_oldest_nonremovable`: The oldest XID for which deleted tuples must be retained in regular user-defined tables
- `temp_oldest_nonremovable`: The oldest XID for which deleted tuples must be retained in the current session's temporary tables
## Dependencies
- Functions called/Symbols referenced:
  - FullTransactionId
  - TransactionId

- Called from (representative examples):
  - ComputeXidHorizons
  - [GetOldestNonRemovableTransactionId](../G/GetOldestNonRemovableTransactionId.md)
  - [GetOldestTransactionIdConsideredRunning](../G/GetOldestTransactionIdConsideredRunning.md)
  - [GetReplicationHorizons](../G/GetReplicationHorizons.md)
  - [GlobalVisUpdateApply](../G/GlobalVisUpdateApply.md)
  - [GlobalVisUpdate](../G/GlobalVisUpdate.md)

## Notes and Other Information
- This structure is the result of expensive computation and should be cached when possible to avoid repeated ProcArrayLock acquisitions
- The different horizon values allow vacuum and other cleanup operations to be as aggressive as possible while maintaining correctness for each relation type
- The distinction between raw and non-raw shared horizons is specifically designed for streaming replication feedback mechanisms
- The temp_oldest_nonremovable horizon is typically the most aggressive since temporary tables are only visible to the current session
- The oldest_considered_running field includes special handling for VACUUM processes that need access to pg_subtrans for visibility determination
- Values in this structure are computed under ProcArrayLock protection to ensure consistency
- The structure enables efficient implementation of the GlobalVisState optimization by providing precise boundaries when needed