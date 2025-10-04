# pg_ts_config_is_visible

## Location
[src/backend/catalog/namespace.c:5062-5075](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L5062-L5075)

## Overview
A PostgreSQL system function that determines whether a text search configuration is visible in the current search path, returning NULL if the configuration does not exist.

## Definition

```c
Datum
pg_ts_config_is_visible(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as a PostgreSQL system function that checks the visibility of a text search configuration within the current schema search path. It takes a configuration OID as input and returns a boolean value indicating whether the configuration is accessible from the current context. The function handles missing configurations gracefully by returning NULL instead of throwing an error, making it suitable for use in SQL queries where the existence of the configuration is uncertain.

The function leverages the internal  function to perform the actual visibility check, which considers the current namespace search path and handles namespace precedence rules to determine if the specified configuration would be found when referenced by name.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0:  - The object identifier of the text search configuration to check

## Dependencies
- Functions called/Symbols referenced:
  -  (macro for extracting OID argument)
  -  (internal visibility checking function)
  -  (macro for returning NULL)
  -  (macro for returning boolean result)
- Called from:
  - Available as SQL system function 

## Notes and Other Information
- Returns boolean  if the configuration is visible,  if not visible, or  if the configuration doesn't exist
- The visibility determination follows PostgreSQL's standard namespace search path resolution
- This function is part of PostgreSQL's text search infrastructure
- Located in 
- The function uses the "Ext" variant of the visibility checker to handle missing objects gracefully
- Similar in structure and purpose to  and  but operates on text search configurations

## Simplified Source

```c
Datum pg_ts_config_is_visible(PG_FUNCTION_ARGS) {
    Oid config_oid = PG_GETARG_OID(0);
    bool is_missing = false;

    // Check if text search configuration is visible in current search path
    bool result = TSConfigIsVisibleExt(config_oid, &is_missing);

    // Return NULL if configuration doesn't exist, otherwise return visibility status
    if (is_missing)
        PG_RETURN_NULL();
    PG_RETURN_BOOL(result);
}
```