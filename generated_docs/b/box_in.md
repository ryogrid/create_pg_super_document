# box_in

## Location
[src/backend/utils/adt/geo_ops.c:422-454](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L422-L454)

## Overview
Converts a string representation of a box geometric data type to its internal PostgreSQL BOX structure format.

## Definition


## Detailed Description
The  function is a PostgreSQL input conversion function that parses string representations of rectangular boxes and converts them to the internal BOX data type. It supports two input formats: the modern format "(f8, f8), (f8, f8)" representing two corner points, and the legacy format "(f8, f8, f8, f8)" representing coordinates in a flat sequence. After parsing, the function automatically reorders the coordinates to ensure that the 'high' point contains the maximum x and y values, and the 'low' point contains the minimum values, maintaining the canonical box representation.

## Parameters / Member Variables
- Uses  macro which provides:
  - : String input containing the box coordinates to be parsed
  - : Error context node for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CSTRING
  - [palloc](../p/palloc.md)
  - path_decode
  - [float8_lt](../f/float8_lt.md)
  - PG_RETURN_BOX_P
  - PG_RETURN_NULL
  - [BOX](../B/BOX.md) (struct type)
- Called from (representative examples):
  - This is a PostgreSQL input function, typically called by the PostgreSQL parser when converting string literals to box type

## Notes and Other Information
This function follows PostgreSQL's standard input function convention using the  interface. It implements automatic coordinate normalization to ensure boxes are always represented with consistent high/low corner semantics regardless of input order. The function supports soft error handling through the error context parameter, allowing it to return NULL on parse errors rather than throwing exceptions in appropriate contexts. The use of  with parameter  and  indicates it expects exactly 2 points for a valid box.