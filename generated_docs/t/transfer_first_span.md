# transfer_first_span

## Location
[src/backend/utils/mmgr/dsa.c:1432-1471](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/dsa.c#L1432-L1471)

## Overview
Transfers the first span from one fullness class to another within a DSA pool, updating the doubly-linked list structure to maintain span organization by allocation density.

## Definition

```c
static bool
transfer_first_span(dsa_area *area,
					dsa_area_pool *pool, int fromclass, int toclass)
```
## Detailed Description
This function implements span migration between different fullness classes within a DSA pool. Fullness classes organize spans based on how many objects are currently allocated within them (empty, partially full, nearly full, completely full). When a span's allocation pattern changes, this function moves it to the appropriate fullness class by manipulating the doubly-linked lists that maintain each class.

The function safely removes the first span from the source fullness class and inserts it at the head of the target fullness class, updating all necessary pointer relationships. This maintains the invariant that spans are organized by their allocation density, enabling efficient allocation strategies.

## Parameters / Member Variables
- : Pointer to the DSA area containing the pool
- : Pointer to the pool containing the span lists
- : Source fullness class index (0=empty, 1=partially full, 2=nearly full, 3=full)
- : Target fullness class index to move the span to

## Dependencies
- Functions called/Symbols referenced:
  - DsaPointerIsValid
  - dsa_get_address
  - InvalidDsaPointer
- Called from (representative examples):
  - alloc_object
  - ensure_active_superblock

## Notes and Other Information
- This is a static internal function used for DSA span management
- Returns  if a span was successfully transferred,  if the source class was empty
- Maintains the doubly-linked list invariants by updating both next and previous pointers
- Updates the span's fullness class field () to reflect its new classification
- Critical for dynamic span management as allocation patterns change
- Handles edge cases like empty lists and single-element lists correctly
- The function operates entirely on pointer manipulation without moving actual data
- Used to implement allocation strategies that prefer spans with specific fullness characteristics
- Essential for maintaining optimal allocation performance by keeping spans organized by utilization