# JsonPathExists

## Location
[src/backend/utils/adt/jsonpath_exec.c:3888-3910](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L3888-L3910)

## Overview
An executor-callable function that implements JSON_EXISTS functionality, determining whether a JSON path expression matches any elements in a JSON document.

## Definition

```c
bool
JsonPathExists(Datum jb, JsonPath *jp, bool *error, List *vars)
```
## Detailed Description
The  function serves as the primary entry point for JSON_EXISTS operations in PostgreSQL's SQL/JSON implementation. It wraps the core  function with appropriate parameters to determine existence rather than extract values. The function operates in two modes: it can either throw errors on path evaluation failures (when error is NULL) or return a controlled error state (when error is not NULL). This dual behavior makes it suitable for both direct SQL usage and internal executor operations where error handling needs to be deferred.

## Parameters / Member Variables
- `jb`: The input JSON document as a PostgreSQL Datum (typically JSONB)
- `*jp`: Pointer to the compiled JsonPath expression to evaluate
- `*error`: Optional output parameter for error state - if NULL, errors are thrown; if not NULL, errors set this to true
- `*vars`: List of variables available during path evaluation (for parameterized paths)
## Dependencies
- Functions called/Symbols referenced:
  - [executeJsonPath](../e/executeJsonPath.md)
  - [GetJsonPathVar](../G/GetJsonPathVar.md)
  - [CountJsonPathVars](../C/CountJsonPathVars.md)
  - [DatumGetJsonbP](../D/DatumGetJsonbP.md)
  - jperIsError
  - jperOk (enum value)
  - [JsonPath](JsonPath.md) (type)
  - [JsonPathExecResult](JsonPathExecResult.md) (type)
- Called from (representative examples):
  - [ExecEvalJsonExprPath](../E/ExecEvalJsonExprPath.md)

## Notes and Other Information
- Returns true if the JSON path exists/matches, false otherwise
- Uses the  parameter to  to control error throwing behavior
- The function expects  to return jperOk on successful existence check
- Part of PostgreSQL's SQL/JSON standard implementation
- Commonly used in WHERE clauses and conditional expressions involving JSON data
- The  parameter supports JSON path expressions with variables (e.g., )

## Simplified Source

```c
bool
JsonPathExists(Datum jb, JsonPath *jp, bool *error, List *vars)
{
    JsonPathExecResult res;

    // Execute JSON path with existence-checking mode
    res = executeJsonPath(jp, vars,
                          GetJsonPathVar, CountJsonPathVars,
                          DatumGetJsonbP(jb), !error, NULL, true);

    // Handle error cases based on error parameter
    if (error && jperIsError(res)) {
        *error = true;
        return false;
    }

    // Return true if path exists (result is jperOk)
    return res == jperOk;
}
```