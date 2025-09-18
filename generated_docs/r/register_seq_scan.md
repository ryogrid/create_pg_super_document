# register_seq_scan

## Location
src/backend/utils/hash/dynahash.c: 1825 - 1836

## Overview
Registers a hash table as having an active sequential scan operation to track resource usage and transaction-level cleanup.

## Definition
```c
static void register_seq_scan(HTAB *hashp)
```

## Detailed Description
This function registers a hash table in the global sequential scan tracking system when a `hash_seq_search` operation begins. It maintains two parallel arrays: one tracking active hash tables and another recording the transaction nesting level at which each scan started. This tracking mechanism is essential for proper cleanup of hash table resources, particularly when transactions are rolled back or when the system needs to manage multiple concurrent sequential scans. The function enforces a maximum limit on concurrent sequential scans to prevent resource exhaustion.

## Parameters / Member Variables
- `hashp`: Pointer to the HTAB structure representing the hash table for which a sequential scan is being registered.

## Dependencies
- Functions called/Symbols referenced:
  - HTAB (struct type)
  - MAX_SEQ_SCANS (constant)
  - GetCurrentTransactionNestLevel
- Called from (representative examples):
  - MOD
  - hash_seq_init

## Notes and Other Information
- This is a static function, only accessible within the dynahash.c file
- Uses global arrays `seq_scan_tables` and `seq_scan_level` to track active scans
- Throws an ERROR if MAX_SEQ_SCANS limit is exceeded, preventing system overload
- The transaction nesting level is recorded to enable proper cleanup during transaction rollbacks
- Part of PostgreSQL's hash table sequential scan infrastructure that ensures resource management and cleanup