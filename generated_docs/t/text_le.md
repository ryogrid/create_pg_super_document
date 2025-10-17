# text_le

## Location
[src/backend/utils/adt/varlena.c:1746-1760](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/varlena.c#L1746-L1760)

## Overview
A PostgreSQL function that implements the "less than or equal to" comparison operator (<=) for the text data type, returning true if the first text argument is lexicographically less than or equal to the second.

## Definition

```c
Datum
text_le(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that performs a "less than or equal to" comparison between two text values. It uses collation-aware comparison through the  function to determine the lexicographic ordering. The function follows PostgreSQL's standard function calling convention for built-in functions, accepting arguments through the  macro and returning a  type. The comparison result is true (1) if the first text argument is lexicographically less than or equal to the second argument, and false (0) otherwise.

## Parameters / Member Variables
- : PostgreSQL's standard macro for function arguments, containing:
  -  (text*): First text value to compare (left operand)
  -  (text*): Second text value to compare (right operand)

## Dependencies
- Functions called/Symbols referenced:
  - : Core text comparison function that performs collation-aware string comparison
  - : Retrieves the collation to use for the comparison
  - : Macro to extract text arguments from function call
  - : Memory management macro to free copied arguments if necessary
  - : Macro to return boolean result as Datum
- Called from (representative examples):
  - No direct references found (typically called through SQL operator <= for text types)

## Notes and Other Information
- This function implements the PostgreSQL <= operator for text data types
- Uses collation-aware comparison, respecting locale-specific sorting rules
- Properly handles memory management by freeing copied arguments after use
- Part of PostgreSQL's comprehensive set of text comparison operators
- The function is defined in  at lines 1746-1760

## Simplified Source
```c
Datum text_le(PG_FUNCTION_ARGS)
{
    // Extract text arguments
    text *text1 = PG_GETARG_TEXT_PP(0);
    text *text2 = PG_GETARG_TEXT_PP(1);

    // Perform comparison: result <= 0 means text1 <= text2
    bool result = (text_cmp(text1, text2, PG_GET_COLLATION()) <= 0);

    // Clean up memory if needed
    PG_FREE_IF_COPY(text1, 0);
    PG_FREE_IF_COPY(text2, 1);

    return PG_RETURN_BOOL(result);
}
```