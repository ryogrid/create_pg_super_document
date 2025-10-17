# jsonb_path_match_opr

## Location
[src/backend/utils/adt/jsonpath_exec.c:514-525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonpath_exec.c#L514-L525)

## Overview
Implementation of the PostgreSQL operator "jsonb @@ jsonpath" that evaluates a JSONPath predicate expression against a JSONB value using operator syntax.

## Definition

```c
Datum
jsonb_path_match_opr(PG_FUNCTION_ARGS)
```
## Detailed Description
`jsonb_path_match_opr` implements the PostgreSQL operator "jsonb @@ jsonpath", which provides a convenient operator-based syntax for evaluating JSONPath predicate expressions against JSONB data. This function serves as the 2-argument version of `jsonb_path_match()`, allowing users to write expressions like `jsonb_value @@ 'path_expression'` instead of calling the function directly.

The function delegates to `jsonb_path_match_internal` with timezone handling disabled (false parameter), making it equivalent in behavior to `jsonb_path_match` but accessible through operator syntax.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:

## Dependencies
- Functions called/Symbols referenced:
  - [jsonb_path_match_internal](jsonb_path_match_internal.md)
- Called from (representative examples):
  - PostgreSQL operator evaluation system when processing "@@" operators

## Notes and Other Information
- Implements the "@@" operator for JSONB and JSONPath
- Provides syntactic sugar for JSONPath predicate matching
- Equivalent to `jsonb_path_match` but with operator syntax
- Does not support timezone-aware datetime operations (unlike `jsonb_path_match_tz`)
- Located in `src/backend/utils/adt/jsonpath_exec.c:514-525`
- Comment indicates this is a 2-argument version of `jsonb_path_match()`

## Simplified Source

```c
Datum jsonb_path_match_opr(PG_FUNCTION_ARGS) {
    // Implementation of "jsonb @@ jsonpath" operator
    // Delegates to internal function without timezone handling
    return jsonb_path_match_internal(fcinfo, false);
}
```