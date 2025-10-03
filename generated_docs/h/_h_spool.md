# _h_spool

## Location
[src/backend/access/hash/hashsort.c:109-119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/hash/hashsort.c#L109-L119)

## Overview
Spools an index entry into the sort file during hash index construction by adding tuples to the sorting state.

## Definition

```c
void
_h_spool(HSpool *hspool, ItemPointer self, const Datum *values, const bool *isnull)
```
## Detailed Description
This function serves as an interface to add index tuples to the sorting mechanism during hash index construction. It takes the tuple data (values and null indicators) along with the tuple identifier and passes them to the underlying tuplesort subsystem via . The function acts as a thin wrapper that maintains the abstraction between the hash index building logic and the generic tuple sorting functionality.

## Parameters / Member Variables
- : Pointer to the HSpool structure managing the sort operation
- : ItemPointer identifying the heap tuple being indexed
- : Array of Datum values representing the index key values
- : Array of boolean flags indicating which values are NULL

## Dependencies
- Functions called/Symbols referenced:
  - [HSpool](../H/HSpool.md) (structure type)
  - [tuplesort_putindextuplevalues](../t/tuplesort_putindextuplevalues.md) (adds tuple to sort state)
- Called from (representative examples):
  - [hashbuildCallback](hashbuildCallback.md)

## Notes and Other Information
- Called repeatedly during the table scan phase of hash index construction
- The actual sorting and spooling to disk is handled by the tuplesort subsystem
- Part of the hash index building pipeline that processes each heap tuple
- Values and isnull arrays must correspond to the index's key attributes

## Simplified Source

```c
void _h_spool(HSpool *hspool, ItemPointer self, const Datum *values, const bool *isnull)
{
    // Add index tuple to sort state for later processing
    tuplesort_putindextuplevalues(hspool->sortstate, hspool->index,
                                  self, values, isnull);
}
```