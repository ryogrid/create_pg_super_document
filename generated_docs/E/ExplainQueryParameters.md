# ExplainQueryParameters

## Location
[src/backend/commands/explain.c:1184-1201](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/explain.c#L1184-L1201)

## Overview
Adds query parameter information to EXPLAIN output as a "Query Parameters" property, displaying the values of parameters used in parameterized queries.

## Definition
```c
void ExplainQueryParameters(ExplainState *es, ParamListInfo params, int maxlen)
```

## Detailed Description
ExplainQueryParameters formats and displays the parameters used in parameterized queries within EXPLAIN output. It uses BuildParamLogString to create a string representation of the parameter values, respecting the specified maximum length limit to prevent excessively long output.

The function performs validation checks to ensure there are parameters to display before attempting to format them. It follows the same validation logic as errdetail_params() for consistency. Only non-empty parameter strings are added to the output.

This function is useful for debugging parameterized queries by showing the actual values that were bound to parameters during query execution, making it easier to understand query behavior and performance characteristics.

## Parameters / Member Variables
- `es`: ExplainState structure containing formatting configuration and output destination
- `params`: ParamListInfo structure containing the parameter values and metadata for the query
- `maxlen`: Maximum length limit for the parameter string representation to prevent excessive output

## Dependencies
- Functions called/Symbols referenced:
  - BuildParamLogString (formats parameter values into a string representation)
  - ExplainPropertyText (adds formatted text property to output)
  - ParamListInfo (parameter list structure)
- Called from (representative examples):
  - Currently no direct callers found in the codebase (utility function for manual use)

## Notes and Other Information
- Public function (not static), available for use throughout the PostgreSQL backend
- Returns early if params is NULL, has no parameters, or maxlen is 0
- Uses consistent validation logic with errdetail_params()
- Only outputs non-empty parameter strings
- Part of PostgreSQLs EXPLAIN infrastructure for displaying query information
- Useful for debugging and understanding parameterized query execution
- Located in src/backend/commands/explain.c:1184-1201