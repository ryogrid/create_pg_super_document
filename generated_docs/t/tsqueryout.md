# tsqueryout

## Location
[src/backend/utils/adt/tsquery.c:1146-1188](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsquery.c#L1146-L1188)

## Overview
The  function is a PostgreSQL I/O function that converts a TSQuery data type into its human-readable text string representation.

## Definition

```c
Datum
tsqueryout(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the output function for the TSQuery data type in PostgreSQL's type system. It takes an internal TSQuery structure and converts it into a human-readable string format that users can understand and work with. The function is responsible for the reverse operation of  - instead of parsing text into TSQuery, it formats TSQuery back into text.

The function handles two main cases:
1. **Empty queries**: When the TSQuery has no operands (size == 0), it returns an empty string
2. **Non-empty queries**: It uses the  function to recursively traverse the query tree and build a properly formatted string representation with correct operator precedence and parentheses

The output format follows standard tsquery syntax with operators like & (AND), | (OR), \! (NOT), and <-> (PHRASE), along with proper handling of operand weights (:A, :B, :C, :D) and prefix markers (:*).

The function follows PostgreSQL's standard I/O function conventions, using the  calling mechanism and returning a C-string through .

## Parameters / Member Variables
- Function uses  calling convention:
  - : Input TSQuery structure to be converted to string

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TSQUERY
  - GETQUERY
  - GETOPERAND
  - [infix](../i/infix.md)
  - PG_RETURN_CSTRING
  - PG_FREE_IF_COPY
  - [palloc](../p/palloc.md)
- Called from (representative examples):
  - PostgreSQL type system (no direct code references found, called via function manager)

## Notes and Other Information
- This is a PostgreSQL I/O function registered in the system catalogs for the TSQuery data type
- Returns an empty string for queries with no operands rather than NULL
- Initializes the INFIX structure with a 32-character initial buffer that grows dynamically as needed
- Uses -1 as the parent priority parameter to , ensuring no unnecessary outer parentheses are added
- Properly manages memory by freeing the input TSQuery if it was a copy (varlena detoasting)
- The function is typically called indirectly through PostgreSQL's type conversion system when TSQuery values need to be displayed or converted to text
- The output string is allocated using palloc and will be automatically freed by PostgreSQL's memory management system