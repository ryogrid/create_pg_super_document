# pltcl_build_tuple_result

## Location
[src/pl/tcl/pltcl.c:3180-3263](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/tcl/pltcl.c#L3180-L3263)

## Overview
Builds a HeapTuple from a Tcl list of column names and values, converting Tcl function return data into PostgreSQL's internal tuple format for functions and triggers.

## Definition
```c
static HeapTuple
pltcl_build_tuple_result(Tcl_Interp *interp, Tcl_Obj **kvObjv, int kvObjc,
                         pltcl_call_state *call_state)
```

## Detailed Description
This function converts a Tcl list containing alternating column names and values into a PostgreSQL HeapTuple. It is used to process return values from PL/Tcl functions and triggers. The function expects the input to be a flat list where each pair of elements represents a column name and its corresponding value. It handles both regular function returns (using the function's return type descriptor) and trigger function returns (using the trigger table's row type).

The function performs extensive validation, checking that the column names exist in the target tuple descriptor, that system attributes and generated columns are not being set, and that the input list has an even number of elements. It uses AttInMetadata to facilitate the conversion from string values to typed PostgreSQL datums, and for domain types, it performs domain constraint checking on the final result.

## Parameters / Member Variables
- `interp`: The Tcl interpreter context (currently unused in the function body)
- `kvObjv`: Array of Tcl objects representing alternating column names and values
- `kvObjc`: Count of elements in the kvObjv array (must be even)
- `call_state`: State information about the current function call, containing type descriptors and context

## Dependencies
- Functions called/Symbols referenced:
  - [TupleDescGetAttInMetadata](../T/TupleDescGetAttInMetadata.md)
  - [utf_u2e](../u/utf_u2e.md)
  - [SPI_fnumber](../S/SPI_fnumber.md)
  - [BuildTupleFromCStrings](../B/BuildTupleFromCStrings.md)
  - [domain_check](../d/domain_check.md)
  - [HeapTupleGetDatum](../H/HeapTupleGetDatum.md)
  - RelationGetDescr
  - TupleDescAttr
  - [palloc0](palloc0.md)
  - ereport
  - elog
  - strcmp
- Called from (representative examples):
  - [pltcl_func_handler](pltcl_func_handler.md)
  - [pltcl_trigger_handler](pltcl_trigger_handler.md)
  - [pltcl_returnnext](pltcl_returnnext.md)

## Notes and Other Information
- The function explicitly leaks memory as noted in the comments, since cleanup is impractical due to datatype input functions also potentially leaking
- It should be run in a short-lived memory context unless the procedure is about to exit
- The function silently ignores ".tupno" fields to allow direct reuse of tuples returned by pltcl_set_tuple_values()
- System attributes and generated columns cannot be set and will cause errors
- For domain-over-composite return types, domain constraints are validated on the final result
- UTF-8 encoding conversion is performed using utf_u2e() to convert from Tcl's UTF-8 to PostgreSQL's internal encoding