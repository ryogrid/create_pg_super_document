# intset_num_entries

## Location
[src/backend/lib/integerset.c:350-358](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/lib/integerset.c#L350-L358)

## Overview
Returns the total number of integer entries stored in an IntegerSet.

## Definition
```c
uint64 intset_num_entries(IntegerSet *intset)
```

## Detailed Description
The `intset_num_entries` function provides a simple accessor to retrieve the total count of integers stored in the IntegerSet. This count includes all integers that have been added to the set, whether they are currently in buffered form awaiting compression or already stored in the compressed B-tree structure. The function returns the value directly from the IntegerSet's `num_entries` field, which is maintained as integers are added to the set.

## Parameters / Member Variables
- `intset`: Pointer to the IntegerSet structure whose entry count is being queried

## Dependencies
- Functions called/Symbols referenced:
  - [IntegerSet](../I/IntegerSet.md): Structure type being accessed
- Called from (representative examples):
  - [gistvacuum_delete_empty_pages](../g/gistvacuum_delete_empty_pages.md): Used in GiST index vacuum operations to check if the set has entries
  - Various test functions in test_integerset module for validation

## Notes and Other Information
- This is a simple O(1) accessor function with no computational overhead
- The returned count represents the total number of unique integers in the set
- The count is automatically maintained by other IntegerSet functions during add operations
- Returns `uint64` to support very large sets of integers
- Does not require flushing buffered values or tree traversal to determine the count

## Simplified Source

```c
uint64 intset_num_entries(IntegerSet *intset) {
    return intset->num_entries;
}
```