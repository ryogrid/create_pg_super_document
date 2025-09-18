# heap_fill_tuple

## Location
[src/backend/access/common/heaptuple.c:400-454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/heaptuple.c#L400-L454)

## Overview
heap_fill_tuple loads the data portion of a PostgreSQL heap tuple from arrays of values and null indicators, filling the null bitmap and setting appropriate infomask bits that reflect the tuple's data contents.

## Definition


## Detailed Description
heap_fill_tuple is a fundamental function in PostgreSQL's tuple construction system. It takes column values and null indicators in array form and serializes them into the binary heap tuple format. The function iterates through all attributes defined in the tuple descriptor, calling fill_val for each attribute to handle the specific storage requirements of different data types.

The function performs several key operations:
- Initializes null bitmap construction if a bitmap is provided
- Clears relevant infomask flags before processing
- Iterates through all tuple attributes in order
- Calls fill_val for each attribute to handle data serialization
- Maintains proper data alignment and handles variable-length attributes
- Updates infomask flags based on the data characteristics encountered

A critical requirement is that the caller must pre-zero the data area before calling this function.

## Parameters / Member Variables
- : TupleDesc structure describing the tuple format and attributes
- : Array of Datum values for each attribute (can be NULL for all-null tuples)
- : Array of boolean flags indicating which attributes are NULL (can be NULL to treat all as NULL)
- : Pre-zeroed buffer where tuple data will be written
- : Expected size of the data to be written (used for assertion checking)
- : Pointer to tuple's info mask that will be updated with tuple characteristics
- : Pointer to null bitmap area (can be NULL if no null bitmap is needed)

## Dependencies
- Functions called/Symbols referenced:
  - TupleDescAttr (access tuple descriptor attributes)
  - [fill_val](../f/fill_val.md) (per-attribute data serialization)
  - [PointerGetDatum](../P/PointerGetDatum.md) (datum conversion for null values)
  - HIGHBIT, HEAP_HASNULL, HEAP_HASVARWIDTH, HEAP_HASEXTERNAL (bitmap and flag constants)
- Called from (representative examples):
  - [heap_form_tuple](heap_form_tuple.md)
  - [heap_form_minimal_tuple](heap_form_minimal_tuple.md)
  - [index_form_tuple_context](../i/index_form_tuple_context.md)
  - [brin_form_tuple](../b/brin_form_tuple.md)
  - [spgFormLeafTuple](../s/spgFormLeafTuple.md)
  - [heap_toast_insert_or_update](heap_toast_insert_or_update.md)

## Notes and Other Information
- Requires caller to pre-zero the data area - this is now a mandatory requirement
- The function handles both cases where null bitmap is needed (bit != NULL) and where it's not
- Uses assertion checking to verify that exactly the expected amount of data was written
- Critical for tuple formation performance as it's used by all tuple construction routines
- The infomask flags set by this function are essential for tuple interpretation during reading
- Handles the complex coordination between null bitmap construction and data serialization
- Part of the core tuple access subsystem used throughout PostgreSQL's storage engine