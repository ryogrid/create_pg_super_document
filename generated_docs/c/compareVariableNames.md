# compareVariableNames

## Location
[src/bin/pgbench/pgbench.c:1596-1603](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pgbench/pgbench.c#L1596-L1603)

## Overview
A qsort comparator function that compares Variable structures by their name field using lexicographic string ordering for efficient variable lookup operations.

## Definition
```c
static int compareVariableNames(const void *v1, const void *v2)
```

## Detailed Description
The `compareVariableNames` function serves as a comparison function for sorting and searching Variable structures in pgbench. It implements the standard qsort/bsearch comparator interface by taking two void pointers, casting them to Variable pointers, and comparing their name fields using strcmp(). This function enables efficient binary search operations on sorted Variable arrays, which is crucial for pgbench's variable lookup performance when dealing with large numbers of variables. The function returns the standard strcmp() result: negative if v1's name is lexicographically less than v2's name, zero if they are equal, and positive if v1's name is greater.

## Parameters / Member Variables
- `v1`: Void pointer to the first Variable structure to compare
- `v2`: Void pointer to the second Variable structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (standard C library string comparison function)
- Data types used:
  - [Variable](../V/Variable.md) (pgbench variable structure containing name field)
- Called from (representative examples):
  - [lookupVariable](../l/lookupVariable.md) (for both qsort and bsearch operations)

## Notes and Other Information
- Implements the standard qsort/bsearch comparator function signature
- Performs lexicographic (alphabetical) ordering of variable names
- Essential for maintaining sorted Variable arrays that enable O(log n) binary search lookup
- Uses const void* parameters as required by qsort/bsearch interface
- Simple wrapper around strcmp() with appropriate type casting
- Located in src/bin/pgbench/pgbench.c:1596-1603 and supports efficient variable management in pgbench