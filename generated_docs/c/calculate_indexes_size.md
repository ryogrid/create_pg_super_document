# calculate_indexes_size

## Location
[src/backend/utils/adt/dbsize.c:451-485](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/dbsize.c#L451-L485)

## Overview
Calculates the total on-disk size of all indexes attached to a given table relation, iterating through each index and summing their storage across all forks.

## Definition
```c
static int64 calculate_indexes_size(Relation rel)
```

## Detailed Description
This function computes the aggregate storage size of all indexes associated with a table by:

1. **Index detection**: Checks if the relation has indexes using the `relhasindex` flag
2. **Index enumeration**: Retrieves the list of all index OIDs associated with the relation
3. **Size calculation**: For each index:
   - Opens the index relation with AccessShareLock
   - Iterates through all fork numbers (0 to MAX_FORKNUM) to include main data, FSM, and VM
   - Calculates size for each fork using `calculate_relation_size`
   - Closes the index relation
4. **Resource cleanup**: Frees the index OID list

The function safely handles cases where it's applied to an index itself (returns zero) or relations without indexes.

## Parameters / Member Variables
- `rel`: Relation pointer to the table whose indexes' sizes are being calculated

## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetIndexList](../R/RelationGetIndexList.md): Gets list of index OIDs for the relation
  - [relation_open](../r/relation_open.md): Opens an index relation with specified lock mode
  - `MAX_FORKNUM`: Maximum fork number constant for iterating through all forks
  - [calculate_relation_size](calculate_relation_size.md): Calculates size of a specific relation fork
  - [relation_close](../r/relation_close.md): Closes the index relation and releases lock
  - [list_free](../l/list_free.md): Frees the allocated index OID list
- Called from (representative examples):
  - [pg_indexes_size](../p/pg_indexes_size.md): SQL function wrapper for index size calculation
  - [calculate_total_relation_size](calculate_total_relation_size.md): Used in total relation size calculation

## Notes and Other Information
- Returns total size in bytes as int64
- Uses AccessShareLock for safe concurrent access to index relations
- Properly manages memory by freeing the index list after use
- Returns 0 when applied to an index relation or relations without indexes
- The function is static, limiting its scope to the dbsize.c compilation unit
- Includes all index storage components (main data, FSM, VM) for comprehensive size reporting