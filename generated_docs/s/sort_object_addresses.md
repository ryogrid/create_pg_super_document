# sort_object_addresses

## Location
[src/backend/catalog/dependency.c:2761-2772](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/dependency.c#L2761-L2772)

## Overview
Sorts the items in an ObjectAddresses array using OID-descending order to ensure newer objects are listed first, primarily for stable regression test outputs.

## Definition

```c
void
sort_object_addresses(ObjectAddresses *addrs)
```
## Detailed Description
The  function provides a standardized way to sort ObjectAddress entries within an ObjectAddresses array. The sorting is performed using the standard library's  function with a custom comparator () that implements OID-descending order.

The primary motivation for this sorting approach is to ensure that newer database objects (which typically have higher OIDs) appear first in the sorted list. This ordering is particularly beneficial for regression testing, as it provides predictable, stable output that doesn't vary between test runs.

The function includes an optimization to only perform sorting when there are multiple references (numrefs > 1), avoiding unnecessary work for single-element arrays.

## Parameters / Member Variables
- : Pointer to the ObjectAddresses array to be sorted in-place

## Dependencies
- Functions called/Symbols referenced:
  - qsort (standard library function)
  - [object_address_comparator](../o/object_address_comparator.md) (custom comparison function)
  - ObjectAddresses (struct type)
  - [ObjectAddress](../O/ObjectAddress.md) (struct type for qsort element type)
- Called from (representative examples):
  - [shdepDropOwned](shdepDropOwned.md) (src/backend/catalog/pg_shdepend.c:1513)

## Notes and Other Information
- The sorting is performed in-place, modifying the original ObjectAddresses array
- Uses OID-descending order as the major sort key, meaning higher OIDs (newer objects) come first
- The function is optimized to skip sorting when there's only one or zero elements
- Primarily designed for regression test stability rather than user-facing functionality
- The comment explicitly warns against using this ordering when object order is determined by user input (e.g., DROP command targets)
- Part of PostgreSQL's object management utilities for dependency handling

## Simplified Source
```c
void
sort_object_addresses(ObjectAddresses *addrs)
{
    // Only sort if there are multiple objects to sort
    if (addrs->numrefs > 1)
        qsort(addrs->refs, addrs->numrefs, sizeof(ObjectAddress), object_address_comparator);
}
```