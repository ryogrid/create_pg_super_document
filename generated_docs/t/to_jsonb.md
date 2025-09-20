# to_jsonb

## Location
[src/backend/utils/adt/jsonb.c:1088-1111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L1088-L1111)

## Overview
A SQL-callable function that converts any PostgreSQL value to its JSONB representation, serving as the main entry point for the to_jsonb() SQL function.

## Definition

```c
Datum
to_jsonb(PG_FUNCTION_ARGS)
```
## Detailed Description
The to_jsonb function is the PostgreSQL SQL function implementation that converts any PostgreSQL data type to its corresponding JSONB format. It follows the standard PostgreSQL function calling convention (PG_FUNCTION_ARGS) and can be called from SQL as to_jsonb(anyvalue). The function extracts the input value and its type from the function call context, categorizes the type to determine the appropriate conversion strategy, and then delegates the actual conversion work to datum_to_jsonb. This function serves as the public interface for JSONB conversion from SQL.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS convention:
  - Argument 0: The value to be converted to JSONB (any PostgreSQL type)
  - Return value: JSONB representation of the input value

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATUM
  - [get_fn_expr_argtype](../g/get_fn_expr_argtype.md)
  - [json_categorize_type](../j/json_categorize_type.md)
  - [datum_to_jsonb](../d/datum_to_jsonb.md)
  - PG_RETURN_DATUM
  - JsonTypeCategory
- Called from (representative examples):
  - No direct references found (called from SQL)

## Notes and Other Information
- This is a PostgreSQL SQL function that can be invoked directly from SQL queries
- Includes input validation to ensure the argument type can be determined
- The function signature follows PostgreSQL's V1 calling convention for SQL functions
- Acts as a wrapper around the core conversion logic in datum_to_jsonb
- Supports conversion of any PostgreSQL data type to JSONB format
- Used extensively in SQL queries for JSON processing and data transformation