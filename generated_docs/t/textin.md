# textin

## Location
[src/backend/utils/adt/varlena.c:579-589](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L579-L589)

## Overview
The  function converts a C-style string (cstring) to PostgreSQL's internal text representation, serving as the input function for the text data type.

## Definition

```c
Datum
textin(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL data type input function that handles the conversion of external string representations into PostgreSQL's internal text format. It takes a null-terminated C string as input and converts it to a  datum using the  utility function. This function is part of PostgreSQL's type system infrastructure and is automatically called when text values need to be parsed from external sources like SQL literals or client input.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: A C-style string () to be converted to text format

## Dependencies
- Functions called/Symbols referenced:
  - : Utility function that performs the actual conversion from C string to text
  - : Macro for returning a text pointer from a PostgreSQL function
- Called from (representative examples):
  - : Used in JSON expression path evaluation

## Notes and Other Information
- This function is registered as the input function for the  data type in PostgreSQL's type system
- It uses PostgreSQL's function call convention with  and 
- The actual string-to-text conversion logic is delegated to the  helper function
- Located in

## Simplified Source

```c
Datum
textin(PG_FUNCTION_ARGS)
{
    // Get input C string from function arguments
    char *inputText = PG_GETARG_CSTRING(0);

    // Convert C string to PostgreSQL text type and return
    PG_RETURN_TEXT_P(cstring_to_text(inputText));
}
``` 