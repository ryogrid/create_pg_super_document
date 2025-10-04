# build_indices

## Location
[src/backend/bootstrap/bootstrap.c:951-967](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/bootstrap/bootstrap.c#L951-L967)

## Overview
Builds all the indexes that have been registered during the bootstrap process by iterating through the index list and constructing each index on its corresponding heap table.

## Definition

```c
void
build_indices(void)
```
## Detailed Description
The  function is a critical part of PostgreSQL's bootstrap initialization process. During bootstrap, system catalog indexes are first registered via  and then built in a separate phase by this function. This two-phase approach is necessary because the indexes themselves have catalog entries that need to be included in the indexes on those catalogs.

The function iterates through the global linked list  (Index List Head), which contains all registered indexes. For each index entry, it:
1. Opens the heap table and index relation without locks (since bootstrapping is single-threaded)
2. Calls  to perform the actual index construction
3. Closes both relations

This deferred index building strategy ensures that all catalog entries, including those for the indexes themselves, are properly included in the final index structures.

## Parameters / Member Variables
This function takes no parameters and operates on global state:
- Uses the global  linked list containing registered index information
- Each  entry contains:
  - : OID of the heap table
  - : OID of the index relation  
  - : IndexInfo structure with index metadata
  - : Pointer to next entry in the linked list

## Dependencies
- Functions called/Symbols referenced:
  - : Opens heap table relations
  - : Opens index relations
  - : Performs the actual index construction
  - : Closes index relations
  - : Closes heap table relations
  - : Global linked list of registered indexes
  - : Lock mode constant (no locking during bootstrap)

- Called from (representative examples):
  - Bootstrap parser () via  grammar rule when processing "BUILD INDICES" command

## Notes and Other Information
- This function is only called during PostgreSQL's bootstrap initialization phase
- No locking is required since bootstrap runs in single-threaded mode
- The index list () is built up during bootstrap by calls to 
- After building all indexes, the function leaves  as NULL since it has consumed the entire list
- The two-phase approach (register then build) is essential for system catalog consistency
- Located in
- Part of the bootstrap subsystem that initializes the PostgreSQL system catalogs

## Simplified Source

```c
void build_indices(void) {
    // Iterate through all registered indexes and build them
    for (; ILHead != NULL; ILHead = ILHead->il_next) {
        // Open heap table and index relations (no locking needed during bootstrap)
        Relation heap = table_open(ILHead->il_heap, NoLock);
        Relation ind = index_open(ILHead->il_ind, NoLock);

        // Build the index on the heap table
        index_build(heap, ind, ILHead->il_info, false, false);

        // Close both relations
        index_close(ind, NoLock);
        table_close(heap, NoLock);
    }
}
```