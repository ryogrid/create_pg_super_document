# mxactMemberComparator

## Location
[src/backend/access/transam/multixact.c:1581-1610](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L1581-L1610)

## Overview
A qsort comparison function used to sort MultiXactMember structures, providing a stable ordering based on transaction ID and status without using wraparound comparison.

## Definition

```c
static int
mxactMemberComparator(const void *arg1, const void *arg2)
```
## Detailed Description
This function serves as a comparator for the standard library's qsort function when sorting arrays of MultiXactMember structures. The function implements a two-level comparison strategy: first by transaction ID (xid), then by status if the XIDs are equal. Importantly, it avoids using wraparound comparison for XIDs because wraparound comparison does not respect the triangle inequality required for proper sorting algorithms. Instead, it uses simple integer comparison which provides any valid sort order that qsort can work with reliably.

The comparison follows standard qsort conventions: returning negative values when the first argument is "less than" the second, positive values when "greater than", and zero when equal.

## Parameters / Member Variables
- : Pointer to the first MultiXactMember structure to compare (cast from void*)
- : Pointer to the second MultiXactMember structure to compare (cast from void*)

## Dependencies
- Functions called/Symbols referenced:
  - [MultiXactMember](../M/MultiXactMember.md) (structure type)
- Called from (representative examples):
  - [mXactCacheGetBySet](mXactCacheGetBySet.md) (via qsort)
  - [mXactCachePut](mXactCachePut.md) (via qsort)
  - debug_elog6 (debugging context)

## Notes and Other Information
- The function explicitly avoids wraparound comparison for transaction IDs to maintain the triangle inequality property required by sorting algorithms
- This comparator ensures consistent ordering of MultiXactMember arrays, which is critical for multixact operations and caching
- The two-level comparison (xid first, then status) provides a stable and deterministic sort order
- Being a static function, it is only accessible within the multixact.c compilation unit