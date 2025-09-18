# AtEOSubXact_HashTables

## Location
src/backend/utils/hash/dynahash.c: 1898 - 1919

## Overview
Cleans up any open hash table sequential scans at the end of a subtransaction, ensuring proper resource management and preventing memory leaks.

## Definition
```c
void AtEOSubXact_HashTables(bool isCommit, int nestDepth)
```

## Detailed Description
This function is part of PostgreSQL's transaction management system specifically designed to handle cleanup of hash table sequential scans when a subtransaction ends. It maintains arrays that track active sequential scans (`seq_scan_tables`, `seq_scan_level`) and ensures that any scans started within a subtransaction are properly cleaned up when that subtransaction completes.

The function searches backward through the scan tracking arrays to identify scans that were started at or deeper than the specified nesting depth. When such scans are found, they are removed from the tracking arrays. If the subtransaction is committing (rather than aborting) and leaked scans are detected, a warning is logged since this indicates a potential resource leak.

The cleanup technique uses array compaction by moving the last element to fill gaps, which is efficient but doesn't maintain order. This is acceptable since the function must check all entries anyway.

## Parameters / Member Variables
- `isCommit`: Boolean flag indicating whether the subtransaction is committing (true) or aborting (false). Used to determine whether to log warnings for leaked scans.
- `nestDepth`: Integer representing the nesting depth of the subtransaction being ended. Scans at this depth or deeper need to be cleaned up.

## Dependencies
- Functions called/Symbols referenced:
  - `elog` (for warning messages)
  - Global arrays: `seq_scan_tables`, `seq_scan_level`, `num_seq_scans`
- Called from (representative examples):
  - `CommitSubTransaction` (src/backend/access/transam/xact.c:5135)
  - `AbortSubTransaction` (src/backend/access/transam/xact.c:5299)

## Notes and Other Information
- This function is part of PostgreSQL's resource cleanup infrastructure for subtransactions
- The warning about leaked scans only appears on commit, not abort, since leaks on abort are expected in error conditions
- The function uses a backward iteration strategy to make array compaction efficient during cleanup
- Related to hash table sequential scanning infrastructure (`hash_seq_search`, `HASH_SEQ_STATUS`)
- Proper cleanup prevents resource leaks that could accumulate over many subtransactions