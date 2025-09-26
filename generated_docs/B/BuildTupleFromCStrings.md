# BuildTupleFromCStrings

## Location
[src/backend/executor/execTuples.c:2222-2310](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execTuples.c#L2222-L2310)

## Overview
BuildTupleFromCStrings constructs a HeapTuple from an array of C string values, converting each string to its appropriate PostgreSQL internal representation using the metadata provided by an AttInMetadata structure.

## Definition
HeapTuple BuildTupleFromCStrings(AttInMetadata *attinmeta, char **values)

## Detailed Description
BuildTupleFromCStrings creates a properly formed PostgreSQL heap tuple from an array of C string representations of attribute values. The function uses the AttInMetadata structure (typically created by TupleDescGetAttInMetadata) to access the necessary type input functions and parameters for converting each string value to its internal Datum representation.

The conversion process involves calling the appropriate input function for each non-dropped attribute, even for NULL values, to properly support domain constraints and validation. NULL string pointers in the input array are interpreted as requests to create NULL fields in the resulting tuple. The function handles dropped attributes by setting them to NULL values with appropriate flags.

After converting all string values to Datums and determining their null status, the function creates the final HeapTuple using heap_form_tuple, then cleans up the temporary memory allocations before returning the completed tuple.

## Parameters / Member Variables
- `attinmeta`: An AttInMetadata structure containing the tuple descriptor and input function information needed for string-to-datum conversion
- `values`: An array of C string pointers representing the values for each attribute, where NULL pointers indicate NULL field values

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md)
  - TupleDescAttr
  - [InputFunctionCall](../I/InputFunctionCall.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - [pfree](../p/pfree.md)

- Called from (representative examples):
  - [mxact](../m/mxact.md)
  - [libpqrcv_processTuples](../l/libpqrcv_processTuples.md)
  - [tt_process_call](../t/tt_process_call.md)
  - [prs_process_call](../p/prs_process_call.md)
  - [pg_get_keywords](../p/pg_get_keywords.md)
  - [ts_process_call](../t/ts_process_call.md)
  - [show_all_settings](../s/show_all_settings.md)
  - [pltcl_build_tuple_result](../p/pltcl_build_tuple_result.md)

## Notes and Other Information
- NULL string pointers in the values array create NULL fields in the resulting tuple
- Input functions are called even for NULL values to support proper domain constraint validation
- Dropped attributes are automatically handled by setting them to NULL regardless of input values
- Memory management includes cleanup of temporary Datum and null indicator arrays
- The function supports both pass-by-value and pass-by-reference data types through the PostgreSQL input function interface
- Commonly used by SRFs, text processing functions, and data import utilities that work with string representations of data
- The returned HeapTuple is allocated in the current memory context and must be freed by the caller when no longer needed