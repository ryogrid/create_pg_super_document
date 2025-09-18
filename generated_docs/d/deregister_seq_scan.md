# deregister_seq_scan

## Location
src/backend/utils/hash/dynahash.c: 1837 - 1857

## Overview
Removes a hash table from the active sequential scan tracking system when a sequential scan operation completes.

## Definition
```c
static void deregister_seq_scan(HTAB *hashp)
```

## Detailed Description
This function removes a hash table from the global sequential scan tracking arrays when a `hash_seq_search` operation is completed or terminated. It searches for the specified hash table in the tracking arrays and removes it by copying the last element to the found position, then decreasing the count. The function uses a backward search strategy since the most recently registered scan is typically the one being terminated (stack-like behavior). If the hash table is not found in the tracking system, it raises an error indicating an inconsistent state.

## Parameters / Member Variables
- `hashp`: Pointer to the HTAB structure representing the hash table to be deregistered from sequential scan tracking.

## Dependencies
- Functions called/Symbols referenced:
  - [HTAB](../H/HTAB.md) (struct type)
- Called from (representative examples):
  - MOD
  - [hash_seq_term](../h/hash_seq_term.md)

## Notes and Other Information
- This is a static function, only accessible within the dynahash.c file
- Uses a backward search optimization based on LIFO (Last In, First Out) access pattern assumption
- Employs a "swap with last element" removal strategy for O(1) removal time complexity
- Raises an ERROR if the hash table is not found, indicating a programming error or corruption
- Works in conjunction with `register_seq_scan` to maintain consistent tracking of active sequential scans
- The removal algorithm maintains array compactness by avoiding gaps in the tracking arrays