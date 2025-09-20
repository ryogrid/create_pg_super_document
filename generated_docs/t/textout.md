# textout

## Location
[src/backend/utils/adt/varlena.c:590-600](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L590-L600)

## Overview
The  function converts PostgreSQL's internal text representation to a C-style string (cstring), serving as the output function for the text data type.

## Definition

```c
Datum
textout(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL data type output function that handles the conversion from PostgreSQL's internal text format to external string representations. It takes a text datum as input and converts it to a null-terminated C string using the  utility function. This function is part of PostgreSQL's type system infrastructure and is automatically called when text values need to be converted to external formats for display, client output, or other purposes.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: A text datum to be converted to C-style string format

## Dependencies
- Functions called/Symbols referenced:
  - : Utility function that performs the actual conversion from text datum to C string
  - : Macro for returning a C string from a PostgreSQL function
- Called from (representative examples):
  - : Used in node tree output processing
  - : Used in sample procedural language function handler
  - : Used in sample procedural language trigger handler

## Notes and Other Information
- This function is registered as the output function for the  data type in PostgreSQL's type system
- It uses PostgreSQL's function call convention with  and 
- The actual text-to-string conversion logic is delegated to the  helper function
- Complementary to the  function, forming the input/output pair for text data type
- Located in 