# JsonPathValue

## Location
[src/backend/utils/adt/jsonpath_exec.c:4005-4089](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L4005-L4089)

## Overview
An executor-callable function that implements JSON_VALUE functionality, extracting scalar values from JSON documents using path expressions with strict single-item and scalar-only validation.

## Definition
```c
JsonbValue *JsonPathValue(Datum jb, JsonPath *jp, bool *empty, bool *error, 
                         List *vars, const char *column_name)
```

## Detailed Description
The `JsonPathValue` function implements the SQL/JSON JSON_VALUE operation, which extracts scalar values from JSON documents. Unlike JSON_QUERY, this function has strict requirements: it must return exactly one item, and that item must be a scalar (string, number, boolean, or null). The function performs comprehensive validation, first ensuring only a single item is returned by the path expression, then verifying that the item is indeed scalar. It handles binary JSONB values by extracting scalar content when the container holds a scalar value. The function supports both error-throwing and controlled error return modes, making it suitable for various execution contexts.

## Parameters / Member Variables
- `jb`: The input JSON document as a PostgreSQL Datum (typically JSONB)
- `jp`: Pointer to the compiled JsonPath expression to evaluate
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
  - JsonContainerIsScalar
  - [JsonbExtractScalar](JsonbExtractScalar.md)
  - IsAJsonbScalar
  - [JsonValueList](JsonValueList.md) (type)
  - [JsonPathExecResult](JsonPathExecResult.md) (type)
  - PG_USED_FOR_ASSERTS_ONLY (macro)
  - jbvBinary, jbvNull (enum values)
- Called from (representative examples):
  - [ExecEvalJsonExprPath](../E/ExecEvalJsonExprPath.md)

## Notes and Other Information
- Returns NULL when no matches are found, when result is JSON null, or on error (with appropriate flags set)
- Enforces strict single-item constraint - throws error if path returns multiple items
- Enforces scalar-only constraint - throws error if result is array or object
- Handles binary JSONB containers by extracting scalar content when appropriate
- Provides context-specific error messages for both column and general usage
- Part of PostgreSQL's SQL/JSON standard compliance
- Used primarily in SELECT clauses to extract scalar values for typed columns
- The function automatically handles JSONB binary representation details