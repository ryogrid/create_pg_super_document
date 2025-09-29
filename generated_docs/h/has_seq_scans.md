# has_seq_scans

## Location
[src/backend/utils/hash/dynahash.c:1858-1871](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/hash/dynahash.c#L1858-L1871)

## Overview
Checks whether a given hash table has any active sequential scan operations currently in progress.

## Definition
```c
static bool has_seq_scans(HTAB *hashp)
```

## Detailed Description
This function searches through the global sequential scan tracking arrays to determine if a specific hash table has any active `hash_seq_search` operations. It performs a linear search through all currently registered sequential scans and returns true if the specified hash table is found. This function is essential for operations that need to ensure hash table consistency, such as rehashing or freezing operations, which cannot be safely performed while sequential scans are active on the table.

## Parameters / Member Variables
- `hashp`: Pointer to the HTAB structure representing the hash table to check for active sequential scans.

## Dependencies
- Functions called/Symbols referenced:
  - [HTAB](../H/HTAB.md) (struct type)
- Called from (representative examples):
  - MOD
  - [hash_search_with_hash_value](hash_search_with_hash_value.md)
  - [hash_freeze](hash_freeze.md)

## Notes and Other Information
- This is a static function, only accessible within the dynahash.c file
- Performs O(n) linear search where n is the number of active sequential scans
- Used as a safety check to prevent operations that could invalidate active sequential scan iterators
- Essential for maintaining hash table integrity during structural modifications
- Works in conjunction with the register/deregister functions to provide complete sequential scan lifecycle management
- The function is crucial for preventing race conditions and data corruption during concurrent access patterns

## Simplified Source

```c
// Simplified version of has_seq_scans
static bool has_seq_scans(HTAB *hashp) {
    // Search through all active sequential scans
    for (int i = 0; i < num_seq_scans; i++) {
        // Check if this scan belongs to our hash table
        if (seq_scan_tables[i] == hashp) {
            return true;  // Found an active scan
        }
    }
    return false;  // No active scans found
}
```

Key simplifications made:
- Added descriptive comments explaining each step
- Used inline variable declaration for clearer scope
- Maintained the essential linear search algorithm
- Preserved the exact logic flow and return behavior