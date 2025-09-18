# expanded_record_set_field_internal

## Location
src/backend/utils/adt/expandedrecord.c: 1112 - 1248

## Overview
Sets the value of a specific field in an expanded record, with support for domain constraint checking, external value handling, and memory management optimizations.

## Definition


## Detailed Description
This function provides the comprehensive backend implementation for field assignment in expanded records. It handles multiple complex scenarios including domain constraint validation, external value detoasting, memory context management, and proper cleanup of replaced values.

The function operates through several key phases:

1. **Constraint Validation**: If the record is of domain type and check_constraints is true, it validates the new value against domain constraints before making any changes.

2. **Record Preparation**: Ensures the record is deconstructed (dvalues/dnulls arrays are valid) and validates the field number is within acceptable range.

3. **Value Processing**: For non-null, non-byval values, it handles:
   - Optional detoasting of external values if expand_external is true
   - Copying the value into the record's memory context using datumCopy
   - Memory context cleanup for any detoasting operations
   - Tracking of external values that might need future inlining

4. **Record Update**: Updates the dvalues and dnulls arrays, invalidates the flattened representation (fvalue), and resets the cached flat_size.

5. **Memory Cleanup**: Safely frees the old field value if it was heap-allocated, with special handling for dummy headers and original flat record data.

The function includes extensive safety checks and optimizations, particularly around memory management and avoiding corruption during updates.

## Parameters / Member Variables
- : Pointer to the ExpandedRecordHeader to modify
- : Field number to set (must be positive for user-defined fields)
- : The new Datum value to assign
- : Whether the new value should be null
- : If true, forces detoasting of external/toasted values
- : If true, validates domain constraints (external callers should always use true)

## Dependencies
- Functions called/Symbols referenced:
  - check_domain_for_new_field
  - deconstruct_expanded_record
  - TupleDescAttr (macro)
  - VARATT_IS_EXTERNAL (macro)
  - get_short_term_cxt
  - detoast_external_attr
  - MemoryContextSwitchTo
  - datumCopy
  - MemoryContextReset
  - DatumGetPointer
  - PointerGetDatum
  - pfree
- Types referenced:
  - ExpandedRecordHeader
  - TupleDesc
  - Form_pg_attribute
  - struct varlena
- Flags manipulated:
  - ER_FLAG_IS_DUMMY
  - ER_FLAG_IS_DOMAIN
  - ER_FLAG_DVALUES_VALID
  - ER_FLAG_DVALUES_ALLOCED
  - ER_FLAG_HAVE_EXTERNAL
  - ER_FLAG_FVALUE_VALID
- Called from (representative examples):
  - expanded_record_set_field (macro wrapper)
  - ER_get_flat_size (for inlining external values)

## Notes and Other Information
- External callers should use the expanded_record_set_field macro, not call this function directly
- The function prevents assignment to system columns (fnumber <= 0)
- Memory management is carefully handled to prevent leaks during repeated field updates
- Domain constraint checking can be disabled for internal operations
- Special handling exists for dummy headers to prevent corruption of main record data
- The function invalidates cached flattened representations when fields are modified
- Detoasting operations use a short-term memory context to prevent memory bloat
- Old field values are safely freed, but only if they're not part of the original flat record