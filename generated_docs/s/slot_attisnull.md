# slot_attisnull

## Location
[src/include/executor/tuptable.h:381-394](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/executor/tuptable.h#L381-L394)

## Overview
An inline function that efficiently checks whether a specific attribute of a TupleTableSlot is null without fetching its actual value.

## Definition
```c
static inline bool
slot_attisnull(TupleTableSlot *slot, int attnum)
```

## Detailed Description
This function provides an efficient way to test if an attribute is null without the overhead of actually fetching the attribute's value. It first ensures that the slot has valid null indicators up to the requested attribute number by calling `slot_getsomeattrs` if necessary, then directly checks the corresponding entry in the slot's `tts_isnull` array.

The function is optimized for performance - if the attribute's null status has already been determined (i.e., `attnum <= slot->tts_nvalid`), it simply returns the cached result without any additional computation.

## Parameters / Member Variables
- `slot`: The TupleTableSlot to check for null attribute
- `attnum`: The attribute number to check (1-based indexing)

## Dependencies
- Functions called/Symbols referenced:
  - slot_getsomeattrs
- Called from (representative examples):
  - ATRewriteTable
  - validateDomainNotNullConstraint
  - ExecConstraints
  - slotAllNulls
  - slotNoNulls
  - ri_NullCheck

## Notes and Other Information
- This is an inline function defined in the header file for performance reasons
- Uses 1-based attribute numbering (attnum > 0 is asserted)
- More efficient than `slot_getattr` when only null checking is needed
- The function converts 1-based attnum to 0-based array index (attnum - 1)
- Commonly used in constraint checking, domain validation, and subplan operations
- Part of PostgreSQL's lazy evaluation system for tuple attributes