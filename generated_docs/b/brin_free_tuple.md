# brin_free_tuple

## Location
[src/backend/access/brin/brin_tuple.c:433-445](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_tuple.c#L433-L445)

## Overview
Frees memory allocated for a tuple created by brin_form_tuple or related BRIN tuple creation functions.

## Definition

```c
void
brin_free_tuple(BrinTuple *tuple)
```
## Detailed Description
This function provides a simple memory deallocation interface for BRIN tuples that were previously created by brin_form_tuple or brin_form_placeholder_tuple. It serves as an abstraction layer over the standard pfree() function, providing a consistent API for BRIN tuple memory management and potentially allowing for future enhancements to the deallocation process if needed.

The function performs a straightforward memory release operation, freeing the entire tuple structure that was allocated as a single memory block during tuple creation.

## Parameters / Member Variables
- `*tuple`: Pointer to the BrinTuple structure to be freed, previously allocated by brin_form_tuple or related functions
## Dependencies
- Functions called/Symbols referenced:
  - [BrinTuple](../B/BrinTuple.md) (structure type)
  - [pfree](../p/pfree.md) (memory deallocation function)
- Called from:
  - [summarize_range](../s/summarize_range.md) (src/backend/access/brin/brin.c:1837, 1838)
  - BrinTupleIsEmptyRange (src/include/access/brin_tuple.h:100)

## Notes and Other Information
- Simple wrapper around pfree() for BRIN tuple deallocation
- Provides API consistency and potential for future memory management enhancements
- Should be used to free tuples created by brin_form_tuple or brin_form_placeholder_tuple
- Part of the standard BRIN tuple lifecycle management along with tuple creation functions
- Essential for preventing memory leaks in BRIN index operations

## Simplified Source

```c
void brin_free_tuple(BrinTuple *tuple) {
    // Simple wrapper around pfree to free BRIN tuple memory
    pfree(tuple);
}
```