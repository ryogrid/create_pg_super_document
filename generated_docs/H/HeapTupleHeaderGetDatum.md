# HeapTupleHeaderGetDatum

## Location
src/backend/executor/execTuples.c: 2311 - 2341

## Overview
Converts a HeapTupleHeader pointer to a Datum, ensuring that any external TOAST references are flattened into inline values to create a proper composite Datum.

## Definition
```c
Datum HeapTupleHeaderGetDatum(HeapTupleHeader tuple)
```

## Detailed Description
HeapTupleHeaderGetDatum converts a HeapTupleHeader pointer to a Datum while enforcing the requirement that composite Datums must not contain external TOAST pointers. The function was originally a simple macro equivalent to PointerGetDatum, but was enhanced to handle TOAST flattening requirements.

The function first checks if the tuple contains any external TOAST pointers using HeapTupleHeaderHasExternal(). If no external references exist, it simply returns the pointer as a Datum. However, if external TOAST pointers are found, it:

1. Looks up the rowtype tuple descriptor using the type information stored in the tuple header
2. Calls toast_flatten_tuple_to_datum() to create a new tuple with all values stored inline
3. Releases the tuple descriptor and returns the flattened result

This function is essential for creating proper composite Datums from heap tuples, particularly in contexts where the data needs to be passed around as a self-contained value without external dependencies.

## Parameters / Member Variables
- `tuple`: A HeapTupleHeader pointer that should be freshly created by heap_form_tuple or similar routines, with a properly blessed rowtype in the tuple descriptor

## Dependencies
- Functions called/Symbols referenced:
  - HeapTupleHeaderHasExternal
  - PointerGetDatum
  - lookup_rowtype_tupdesc
  - HeapTupleHeaderGetTypeId
  - HeapTupleHeaderGetTypMod
  - toast_flatten_tuple_to_datum
  - HeapTupleHeaderGetDatumLength
  - ReleaseTupleDesc
- Called from (representative examples):
  - populate_composite (jsonfuncs.c)
  - populate_recordset_record (jsonfuncs.c)
  - PG_RETURN_HEAPTUPLEHEADER macro
  - HeapTupleGetDatum

## Notes and Other Information
- The input tuple must not be an on-disk tuple but should be freshly constructed
- The tuple descriptor used to build the tuple must have a properly blessed rowtype
- If a new tuple is constructed during flattening, it is allocated in the current memory context
- Performance-critical callers should consider ensuring no TOAST pointers exist in heap_form_tuple output to avoid the overhead
- This represents a transition from the original macro implementation to handle modern TOAST requirements
- The function includes extensive commentary about potential future improvements to create composite Datums more directly