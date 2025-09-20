# multirange_in

## Location
[src/backend/utils/adt/multirangetypes.c:117-298](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L117-L298)

## Overview
Converts a string representation to a PostgreSQL multirange value, parsing curly bracket-delimited lists of ranges separated by commas.

## Definition

```c
Datum
multirange_in(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is the input function for PostgreSQL multirange types, responsible for converting text representations into internal multirange format. It expects input in the format  where:

- The entire multirange is bounded by curly braces 
- Individual ranges are separated by commas
- Empty multiranges are represented as 
- Each range follows standard range syntax  or 
- Empty ranges are represented with the literal "empty"
- Whitespace is accepted around braces and commas

The function implements a comprehensive state machine parser that handles:
- Quoted strings within range bounds
- Backslash escaping (both inside and outside quotes)  
- Double-quote escaping within quoted strings
- Proper validation of multirange syntax

The parser delegates individual range parsing to the underlying range type's input function while handling the multirange-specific syntax and structure.

## Parameters / Member Variables
- : String representation of the multirange to parse
- : OID of the multirange type being created
- : Type modifier for the multirange type
- : Error context for soft error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [get_multirange_io_data](../g/get_multirange_io_data.md)
  - [pg_strncasecmp](../p/pg_strncasecmp.md)
  - [pnstrdup](../p/pnstrdup.md)
  - [repalloc](../r/repalloc.md)
  - [InputFunctionCallSafe](../I/InputFunctionCallSafe.md)
  - DatumGetRangeTypeP
  - RangeIsEmpty
  - [make_multirange](make_multirange.md)
  - PG_RETURN_MULTIRANGE_P
- Called from:
  - PostgreSQL type system (input function registration)

## Notes and Other Information
- Uses a finite state machine with states: BEFORE_RANGE, IN_RANGE, AFTER_RANGE, IN_RANGE_QUOTED, IN_RANGE_ESCAPED, IN_RANGE_QUOTED_ESCAPED, FINISHED
- Empty ranges within the multirange are filtered out during construction
- Supports soft error reporting through error context
- Memory management uses palloc/repalloc for dynamic range array allocation
- Initial capacity for ranges is 8, doubled when exceeded
- Comprehensive error messages provide specific details about parsing failures