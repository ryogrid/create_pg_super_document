# vac_tid_reaped

## Location
src/backend/commands/vacuum.c: 2584 - 2589

## Overview
A callback function that determines whether a specific tuple identifier (TID) has been marked for deletion during vacuum operations, serving as an IndexBulkDeleteCallback for index bulk deletion processes.

## Definition
```c
static bool
vac_tid_reaped(ItemPointer itemptr, void *state)
```

## Detailed Description
The `vac_tid_reaped` function is a specialized callback designed to work with PostgreSQL's index bulk deletion mechanism. It serves as a predicate function that determines whether a given tuple identifier (TID) should be considered "reaped" or deleted during vacuum operations.

This function is specifically designed to conform to the `IndexBulkDeleteCallback` signature, making it suitable for use with index access methods that support bulk deletion. The function operates by checking membership of the provided TID in a TidStore data structure that contains all tuple identifiers marked for deletion.

The function is typically used during the bulk deletion phase of vacuum operations, where the vacuum process needs to identify which index entries correspond to dead heap tuples that should be removed from the index.

## Parameters / Member Variables
- `itemptr`: ItemPointer (TID) representing the tuple identifier to check for deletion status
- `state`: Void pointer that is cast to a TidStore containing the collection of dead tuple identifiers; this represents the context state passed to the callback

## Dependencies
- Functions called/Symbols referenced:
  - `[TidStore](../T/TidStore.md)`: Data structure type for efficiently storing and managing collections of tuple identifiers
  - `[TidStoreIsMember](../T/TidStoreIsMember.md)`: Function to check if a specific TID exists in the TidStore collection
  - `ItemPointer`: PostgreSQL type representing a tuple identifier (block number + offset)

- Called from (representative examples):
  - `[vac_bulkdel_one_index](vac_bulkdel_one_index.md)`: Uses this function as a callback during index bulk deletion operations

## Notes and Other Information
- The function is declared as static, indicating it's only used within the vacuum.c compilation unit
- Returns true if the TID is present in the dead items collection (should be deleted), false otherwise
- This callback-based approach allows different index access methods to efficiently process bulk deletions according to their internal organization
- The use of TidStore provides an efficient way to check membership for potentially large sets of dead tuple identifiers
- The function signature exactly matches the `IndexBulkDeleteCallback` typedef, enabling seamless integration with the index AM interface
- This is a critical component in PostgreSQL's vacuum subsystem for maintaining consistency between heap and index during cleanup operations