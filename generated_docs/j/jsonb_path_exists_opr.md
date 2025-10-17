# jsonb_path_exists_opr

## Location
[src/backend/utils/adt/jsonpath_exec.c:444-455](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L444-L455)

## Overview
PostgreSQL operator function that implements the "@?" operator for checking JSONPath existence against JSONB values, providing a 2-argument operator interface.

## Definition

```c
Datum
jsonb_path_exists_opr(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the PostgreSQL "@?" operator, which provides syntactic sugar for JSONPath existence checking. It serves as the operator implementation for expressions like . The function is specifically designed as a 2-argument version of , making it suitable for use as an infix operator.

The function directly delegates to  with timezone awareness disabled, making it equivalent to the standard  function but accessible through operator syntax. This operator form is particularly useful for creating indexable conditions and provides a more natural SQL syntax for JSONPath existence queries.

## Parameters / Member Variables
The function uses PostgreSQL's operator function argument mechanism:
- Arguments are accessed through the  parameter passed to the internal function
- Argument 0: JSONB document (left operand of the @? operator)
- Argument 1: JSONPath expression (right operand of the @? operator)
- No optional arguments supported in the operator form (unlike the function form)

## Dependencies
- Functions called/Symbols referenced:
  -  - The internal implementation function with timezone support disabled

- Called from:
  - This function is called by PostgreSQL's operator execution system when the @? operator is used
  - No direct C function references found in the codebase

## Notes and Other Information
- This function specifically implements the "@?" operator as documented in the comment
- The function is designed as a 2-argument version, making it simpler than the full  function which supports optional parameters
- The operator form provides a more intuitive syntax for JSONPath existence checks in SQL queries
- Like , this function operates without timezone awareness (passes  to the internal function)
- The operator is particularly useful for creating indexable conditions in WHERE clauses
- Part of PostgreSQL's JSONPath operator ecosystem that supports SQL/JSON standard operations
- The comment explicitly states it "can handle both cases", referring to the internal function's ability to handle different argument counts
- Provides syntactic consistency with other PostgreSQL JSONB operators like @>, <@, etc.

## Simplified Source

```c
Datum
jsonb_path_exists_opr(PG_FUNCTION_ARGS)
{
    /* Implementation of operator "jsonb @? jsonpath" (2-argument version) */
    return jsonb_path_exists_internal(fcinfo, false);
}
```