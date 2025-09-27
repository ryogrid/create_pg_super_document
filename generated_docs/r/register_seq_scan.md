# register_seq_scan

## Location
[src/backend/utils/hash/dynahash.c:1825-1836](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L1825-L1836)

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
  - [HTAB](../H/HTAB.md) (struct type)
  - MAX_SEQ_SCANS (constant)
  - [GetCurrentTransactionNestLevel](../G/GetCurrentTransactionNestLevel.md)
- Called from (representative examples):
  - MOD
  - [hash_seq_init](../h/hash_seq_init.md)

## Notes and Other Information
- This is a static function, only accessible within the dynahash.c file
- Uses global arrays `seq_scan_tables` and `seq_scan_level` to track active scans
- Throws an ERROR if MAX_SEQ_SCANS limit is exceeded, preventing system overload
- The transaction nesting level is recorded to enable proper cleanup during transaction rollbacks
- Part of PostgreSQL's hash table sequential scan infrastructure that ensures resource management and cleanup

## Simplified Source

```c
// Simplified version of register_seq_scan
static void register_seq_scan(HTAB *hashp) {
    // Check if we've reached the maximum number of concurrent scans
    if (num_seq_scans >= MAX_SEQ_SCANS) {
        elog(ERROR, "too many active hash_seq_search scans, cannot start one on \"%s\"",
             hashp->tabname);
    }

    // Register the hash table in the tracking arrays
    seq_scan_tables[num_seq_scans] = hashp;
    seq_scan_level[num_seq_scans] = GetCurrentTransactionNestLevel();

    // Increment the count of active scans
    num_seq_scans++;
}
```

Key simplifications made:
- Added explanatory comments for each logical step
- Grouped the core operations into clear sections
- Preserved all original functionality and error handling
- Maintained the exact same logic flow for resource tracking