# get_current_ts_config

## Location
[src/backend/tsearch/to_tsany.c:48-56](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/to_tsany.c#L48-L56)

## Overview
This function is a PostgreSQL built-in function that returns the Object Identifier (OID) of the currently configured default text search configuration.

## Definition

```c
Datum
get_current_ts_config(PG_FUNCTION_ARGS)
```
## Detailed Description
 is a simple wrapper function that provides SQL-level access to PostgreSQL's current text search configuration. It calls the internal  function with the  parameter set to , meaning it will raise an error if no text search configuration is currently set. The function returns the OID of the active text search configuration, which is determined by the  GUC (Grand Unified Configuration) parameter.

This function is typically exposed as an SQL function that can be called from within PostgreSQL queries to determine which text search configuration is currently active for operations like  and  when no explicit configuration is specified.

## Parameters / Member Variables
This function follows the PostgreSQL function calling convention:
- Takes  as parameter (standard PostgreSQL function signature)
- Returns a  containing the OID

## Dependencies
- Functions called/Symbols referenced:
  - : Internal function that retrieves the current text search configuration OID
  - : PostgreSQL macro to return an OID value as a Datum
- Called from (representative examples):
  - Available as SQL function, no direct internal callers found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's text search functionality
- The returned OID corresponds to an entry in the  system catalog
- The function will error if no default text search configuration has been set
- Located in  at lines 48-56
- This is likely registered as a built-in SQL function accessible from user queries

## Simplified Source

```c
Datum
get_current_ts_config(PG_FUNCTION_ARGS)
{
    // Return OID of current text search configuration
    PG_RETURN_OID(getTSCurrentConfig(true));
}
```