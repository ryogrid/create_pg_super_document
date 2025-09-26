# record_out

## Location
[src/backend/utils/adt/rowtypes.c:329-479](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rowtypes.c#L329-L479)

## Overview
Converts the internal binary representation of a composite type (record) into its string representation for PostgreSQL output.

## Definition

```c
structure */
	tuple.t_len = HeapTupleHeaderGetDatumLength(rec);
```
## Detailed Description
The  function serves as the output conversion function for any composite type in PostgreSQL. It takes a  (the internal binary format) and converts it to a human-readable string representation in the format . The function handles proper quoting of values that contain special characters, escape sequence generation, and null value representation.

The function extracts type information directly from the tuple header, decomposes the tuple into individual column values, and formats each value using the appropriate type-specific output function. It implements intelligent quoting logic to only add quotes when necessary and properly escapes quotes and backslashes within values.

## Parameters / Member Variables
- : Input  containing the binary representation of the record to be converted

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md): Stack overflow protection for recursive calls
  - HeapTupleHeaderGetTypeId: Extracts type OID from tuple header
  - HeapTupleHeaderGetTypMod: Extracts type modifier from tuple header
  - [lookup_rowtype_tupdesc](../l/lookup_rowtype_tupdesc.md): Retrieves tuple descriptor for the record type
  - [heap_deform_tuple](../h/heap_deform_tuple.md): Extracts individual column values from tuple
  - [getTypeOutputInfo](../g/getTypeOutputInfo.md): Gets output function info for column types
  - [OutputFunctionCall](../O/OutputFunctionCall.md): Calls type-specific output functions
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md): Memory allocation in function context
  - ReleaseTupleDesc: Releases tuple descriptor reference

- Called from (representative examples):
  - Type system as registered output function for composite types
  - SQL result formatting when displaying record values

## Notes and Other Information
- Implements smart quoting: only quotes values containing special characters (quotes, backslashes, parentheses, commas, or whitespace)
- Properly escapes quotes and backslashes by doubling them within quoted values
- Forces quotes for empty strings to distinguish from null values
- Handles dropped columns by skipping them in output
- Uses function-local caching (fn_extra) to optimize repeated calls with same type
- Memory management ensures result string is properly allocated for caller