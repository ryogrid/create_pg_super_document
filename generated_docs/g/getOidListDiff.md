# getOidListDiff

## Location
[src/backend/catalog/pg_shdepend.c:421-490](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L421-L490)

## Overview
A helper function that computes the difference between two sorted OID arrays, removing common elements and leaving only those unique to each array.

## Definition
```c
static void getOidListDiff(Oid *list1, int *nlist1, Oid *list2, int *nlist2)
```

## Detailed Description
This static utility function performs an in-place difference operation on two sorted and de-duplicated OID arrays. It uses a two-pointer technique to efficiently traverse both arrays simultaneously, identifying elements that exist in only one array or the other. Common elements are skipped, while unique elements are preserved in their original arrays but compacted to the beginning. The function modifies both input arrays and their corresponding length counters to reflect the results.

The algorithm works by maintaining separate input and output pointers for each array, comparing elements at the current input positions and either skipping duplicates or copying unique elements to the output positions.

## Parameters / Member Variables
- `list1`: Pointer to the first sorted OID array, modified in-place to contain elements unique to this array
- `nlist1`: Pointer to the count of elements in list1, updated to reflect the number of unique elements remaining
- `list2`: Pointer to the second sorted OID array, modified in-place to contain elements unique to this array  
- `nlist2`: Pointer to the count of elements in list2, updated to reflect the number of unique elements remaining

## Dependencies
- Functions called/Symbols referenced:
  - None (pure algorithmic function)
- Called from (representative examples):
  - [updateAclDependenciesWorker](../u/updateAclDependenciesWorker.md)

## Notes and Other Information
- Requires both input arrays to be pre-sorted and de-duplicated for correct operation
- Modifies input arrays in-place for memory efficiency
- Uses O(n+m) time complexity where n and m are the sizes of the input arrays
- Static function only accessible within pg_shdepend.c
- Essential helper for ACL dependency management operations