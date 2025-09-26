# add_span_to_fullness_class

## Location
src/backend/utils/mmgr/dsa.c: 1929 - 1951

## Overview
Adds a span to the head of a specified fullness class list, updating all necessary pointers to maintain the doubly-linked list structure.

## Definition

```c
static void
add_span_to_fullness_class(dsa_area *area, dsa_area_span *span,
						   dsa_pointer span_pointer,
						   int fclass)
```
## Detailed Description
This function implements the insertion of a span at the head of a fullness class list within a DSA memory pool. Fullness classes organize spans based on their utilization level, allowing the allocator to efficiently select appropriate allocation targets.

The function performs the standard doubly-linked list head insertion: it updates the current head's prevspan pointer (if a head exists), sets the new span's prevspan to invalid and nextspan to the current head, updates the pool's spans array to point to the new span, and finally records the fullness class in the span's fclass field.

This operation is typically used when a span's utilization changes (due to allocations or deallocations) and it needs to be moved to a different fullness class, or when a newly created span is being added to the appropriate fullness class for the first time.

## Parameters / Member Variables
- : The DSA area containing the span and pool structures
- : Pointer to the span structure to be added to the list
- : DSA pointer value corresponding to the span
- : The target fullness class (0 to DSA_FULLNESS_CLASSES-1) where the span should be added

## Dependencies
- Functions called/Symbols referenced:
  - dsa_get_address
  - DsaPointerIsValid
  - InvalidDsaPointer
- Called from (representative examples):
  - dsa_free

## Notes and Other Information
The function assumes the span is not currently in any fullness class list and does not perform unlinking from a previous list - callers should use unlink_span() first if needed. Like unlink_span(), this function does not handle locking and relies on callers to ensure proper synchronization. The span is always added at the head of the list, making it the first candidate for future allocations in that fullness class.
