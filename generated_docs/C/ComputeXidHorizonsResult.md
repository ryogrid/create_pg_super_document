# ComputeXidHorizonsResult

## Location
[src/backend/storage/ipc/procarray.c:179-244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/procarray.c#L179-L244)

## Overview
ComputeXidHorizonsResult is a structure that contains comprehensive transaction visibility horizon information computed by ComputeXidHorizons(), providing different XID boundaries for various types of relations and operations.

## Definition


## Detailed Description
ComputeXidHorizonsResult encapsulates the comprehensive set of transaction visibility horizons computed by the ComputeXidHorizons() function. This structure is crucial for determining which deleted tuples can be safely removed during vacuum and other cleanup operations while respecting MVCC visibility rules for different classes of relations.

The structure provides fine-grained control over tuple retention by distinguishing between different types of tables (shared, catalog, data, temporary) and different types of operations (replication, visibility checking, subtransaction tracking). Each horizon represents the oldest transaction ID that might still need to see tuples deleted by newer transactions in the corresponding context.

The differentiation between raw and non-raw horizons allows the system to handle replication requirements separately from local visibility requirements, which is essential for streaming replication and logical replication scenarios.

## Parameters / Member Variables
- : The most recent transaction ID that was completed when ComputeXidHorizons() acquired ProcArrayLock, providing a reference point for the computation
- : The oldest transaction ID that any replication slot still needs for data visibility
- : The oldest catalog transaction ID that any replication slot still needs for DDL change visibility
- : The oldest XID that any backend (including VACUUM) might still consider running, primarily used for pg_subtrans truncation decisions
- : The oldest XID for which deleted tuples must be retained in shared catalog tables, including replication slot effects
- : Similar to shared_oldest_nonremovable but excluding replication slot catalog_xmin effects, used for hot_standby_feedback
- : The oldest XID for which deleted tuples must be retained in database-specific catalog tables
- : The oldest XID for which deleted tuples must be retained in regular user-defined tables
- : The oldest XID for which deleted tuples must be retained in the current session's temporary tables

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