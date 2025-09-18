# vac_truncate_clog

## Location
src/backend/commands/vacuum.c: 1804 - 1972

## Overview
Attempts to truncate transaction commit logs (pg_xact), commit timestamps, and MultiXact logs by scanning all databases to find the system-wide oldest datfrozenxid and datminmxid values.

## Definition


## Detailed Description
This function performs critical system maintenance by truncating various transaction-related logs when it's safe to do so. The process involves:

1. **Cluster-wide Locking**: Acquires WrapLimitsVacuumLock to ensure only one backend per cluster performs this operation
2. **Database Scanning**: Scans all pg_database entries to find the system-wide minimum datfrozenxid and datminmxid values
3. **Safety Validation**: Checks for wraparound conditions and "future" transaction IDs that indicate corruption
4. **Log Truncation**: Truncates pg_xact (CLOG), commit timestamps, and MultiXact logs based on the computed minimums
5. **Limit Updates**: Updates transaction ID wrap limits maintained by varsup.c to prevent wraparound

The function implements multiple safety mechanisms including detection of already-wrapped transactions and bogus data. It ensures that commit timestamp lookups return NULL rather than file errors for truncated transactions by advancing the oldest commit timestamp XID before truncation.

## Parameters / Member Variables
- : The updated datfrozenxid value for the current database, used to initialize minimum calculations
- : The updated datminmxid value for the current database, used to initialize minimum calculations  
- : The latest valid frozen XID that could be seen during the scan (used for corruption detection)
- : The latest valid minimum MultiXactId that could be seen during the scan (used for corruption detection)

## Dependencies
- Functions called/Symbols referenced:
  - ReadNextTransactionId
  - table_beginscan_catalog
  - heap_getnext
  - TransactionIdIsNormal
  - MultiXactIdIsValid
  - database_is_invalid_form
  - TransactionIdPrecedes
  - MultiXactIdPrecedes
  - table_endscan
  - AdvanceOldestCommitTsXid
  - TruncateCLOG
  - TruncateCommitTs
  - TruncateMultiXact
  - SetTransactionIdLimit
  - SetMultiXactIdLimit
- Called from (representative examples):
  - vac_update_datfrozenxid

## Notes and Other Information
- This is a static function, only called from within the same source file
- Uses exclusive locking (WrapLimitsVacuumLock) to prevent concurrent truncation operations across the cluster
- Implements a "chicken out" strategy when detecting potentially corrupt data ("future" transaction IDs)
- Skips invalid databases that are in the process of being dropped or have been interrupted during dropping
- Issues warnings for potential transaction wraparound scenarios but continues safely
- The function assumes that fetching/updating XIDs in shared storage is atomic
- Handles race conditions gracefully - concurrent VACUUM operations at worst case result in less aggressive truncation
- Updates wrap limits for both transaction IDs and MultiXactIds, which may signal the postmaster for additional autovacuum cycles
- Advances commit timestamp tracking before truncation to provide better user experience (NULL instead of file errors)