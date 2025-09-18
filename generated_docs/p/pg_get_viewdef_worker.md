# pg_get_viewdef_worker

## Location
src/backend/utils/adt/ruleutils.c: 768 - 850

## Overview
The core worker function that retrieves and formats the SQL definition of a view by accessing the PostgreSQL system catalogs through SPI.

## Definition
```c
static char *pg_get_viewdef_worker(Oid viewoid, int prettyFlags, int wrapColumn)
```

## Detailed Description
This internal worker function implements the core logic for retrieving view definitions in PostgreSQL. It connects to the SPI (Server Programming Interface) manager to execute a prepared query against the pg_rewrite system catalog to find the view's SELECT rule. The function handles SPI connection management, prepares and caches the query plan on first use, retrieves the rule tuple, and delegates to `make_viewdef` for the actual formatting of the view definition. This function serves as the common backend for all the public pg_get_viewdef variants.

## Parameters / Member Variables
- `viewoid`: OID of the view whose definition is to be retrieved
- `prettyFlags`: Integer flags controlling the formatting style of the output
- `wrapColumn`: Integer specifying the column width for line wrapping
- `args`: Array for SPI query parameters (viewoid and rule name)
- `nulls`: Array indicating null status of SPI parameters
- `spirc`: SPI return code for error checking
- `ruletup`: HeapTuple containing the retrieved pg_rewrite rule
- `rulettc`: TupleDesc describing the structure of the rule tuple
- `buf`: StringInfoData buffer for building the result string

## Dependencies
- Functions called/Symbols referenced:
  - initStringInfo
  - SPI_connect
  - [SPI_prepare](../S/SPI_prepare.md)
  - [SPI_keepplan](../S/SPI_keepplan.md)
  - [SPI_execute_plan](../S/SPI_execute_plan.md)
  - SPI_finish
  - DirectFunctionCall1
  - namein
  - [CStringGetDatum](../C/CStringGetDatum.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [make_viewdef](../m/make_viewdef.md)
  - ViewSelectRuleName
- Called from (representative examples):
  - [pg_get_viewdef](pg_get_viewdef.md)
  - [pg_get_viewdef_ext](pg_get_viewdef_ext.md)
  - [pg_get_viewdef_wrap](pg_get_viewdef_wrap.md)
  - [pg_get_viewdef_name](pg_get_viewdef_name.md)
  - [pg_get_viewdef_name_ext](pg_get_viewdef_name_ext.md)

## Notes and Other Information
- This function is part of PostgreSQL's rule utilities system
- Located in src/backend/utils/adt/ruleutils.c:768-850
- Uses SPI instead of syscache to ensure proper access control checking
- Implements query plan caching for performance optimization
- Returns NULL if no view definition is found or if the buffer is empty
- Handles SPI connection lifecycle properly with error checking
- The actual formatting work is delegated to the make_viewdef function
- Uses a static plan_getviewrule variable for caching the prepared statement