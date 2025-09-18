# JsonPathQuery

## Location
src/backend/utils/adt/jsonpath_exec.c: 3911 - 4004

## Overview
An executor-callable function that implements JSON_QUERY functionality, extracting JSON values from a document using a JSON path expression with sophisticated wrapping behavior control.

## Definition
```c
Datum JsonPathQuery(Datum jb, JsonPath *jp, JsonWrapper wrapper, bool *empty,
                   bool *error, List *vars, const char *column_name)
```

## Detailed Description
The `JsonPathQuery` function implements the SQL/JSON JSON_QUERY operation, which extracts values from JSON documents using path expressions. Unlike JSON_EXISTS, this function returns the actual matched values rather than just existence. The function features sophisticated result wrapping logic controlled by the JsonWrapper parameter, supporting four different wrapping modes: JSW_NONE (no wrapping), JSW_UNSPEC (unspecified, defaults to no wrapping), JSW_UNCONDITIONAL (always wrap in array), and JSW_CONDITIONAL (wrap only if multiple items). The function handles error reporting in two modes: immediate exception throwing or controlled error state return, making it suitable for both direct SQL usage and internal executor operations.

## Parameters / Member Variables
- `jb`: The input JSON document as a PostgreSQL Datum (typically JSONB)
- `jp`: Pointer to the compiled JsonPath expression to evaluate
- `wrapper`: JsonWrapper enum controlling how results are wrapped (none, conditional, unconditional)  
- `empty`: Output parameter set to true if no matches are found
- `error`: Optional output parameter for error state - if NULL, errors are thrown; if not NULL, errors set this to true
- `vars`: List of variables available during path evaluation (for parameterized paths)
- `column_name`: Optional column name for improved error messages in table contexts

## Dependencies
- Functions called/Symbols referenced:
  - [executeJsonPath](../e/executeJsonPath.md)
  - [GetJsonPathVar](../G/GetJsonPathVar.md)
  - [CountJsonPathVars](../C/CountJsonPathVars.md)
  - [DatumGetJsonbP](../D/DatumGetJsonbP.md)
  - jperIsError
  - [JsonValueListLength](JsonValueListLength.md)
  - [JsonValueListHead](JsonValueListHead.md)
  - [wrapItemsInArray](../w/wrapItemsInArray.md)
  - [JsonbValueToJsonb](JsonbValueToJsonb.md)
  - [JsonbPGetDatum](JsonbPGetDatum.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - JsonWrapper (enum: JSW_NONE, JSW_UNSPEC, JSW_UNCONDITIONAL, JSW_CONDITIONAL)
  - [JsonValueList](JsonValueList.md) (type)
  - [JsonPathExecResult](JsonPathExecResult.md) (type)
- Called from (representative examples):
  - [ExecEvalJsonExprPath](../E/ExecEvalJsonExprPath.md)

## Notes and Other Information
- Returns NULL (PointerGetDatum(NULL)) when no matches are found and sets *empty to true
- Enforces single-item constraint when no wrapping is requested - throws error if multiple items found
- Supports both column-specific and generic error messages depending on context
- The wrapping behavior implements SQL/JSON standard WITH/WITHOUT WRAPPER clauses
- Part of PostgreSQL's comprehensive SQL/JSON implementation
- Used in SELECT clauses to extract JSON values into result sets