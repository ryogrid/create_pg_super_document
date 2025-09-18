# pg_get_ruledef_worker

## Location
src/backend/utils/adt/ruleutils.c: 575 - 656

## Overview
Core worker function that performs the actual retrieval and formatting of PostgreSQL rewrite rule definitions from the system catalog.

## Definition
```c
static char *pg_get_ruledef_worker(Oid ruleoid, int prettyFlags)
```

## Detailed Description
This is the main implementation function for rule definition retrieval in PostgreSQL. It connects to the SPI (Server Programming Interface) to query the pg_rewrite system catalog, retrieves the rule tuple, and formats it into a readable SQL definition. The function uses a prepared statement for efficient repeated access to rule definitions and handles all aspects of database connectivity, error checking, and memory management.

The function implements a caching mechanism for the SPI plan to avoid repeated preparation costs and ensures proper cleanup of database connections.

## Parameters / Member Variables
- `ruleoid`: OID of the rewrite rule to retrieve from pg_rewrite catalog
- `prettyFlags`: Integer flags controlling the formatting style of the output

## Dependencies
- Functions called/Symbols referenced:
  - `SPI_connect/SPI_finish` - Database connection management
  - `[SPI_prepare](../S/SPI_prepare.md)/SPI_keepplan` - Prepared statement management
  - [SPI_execute_plan](../S/SPI_execute_plan.md) - [Query](../Q/Query.md) execution
  - [make_ruledef](../m/make_ruledef.md) - Core rule formatting function
  - `initStringInfo` - String buffer initialization
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) - OID to Datum conversion
- Called from (representative examples):
  - [pg_get_ruledef](pg_get_ruledef.md) - Basic rule definition retrieval
  - [pg_get_ruledef_ext](pg_get_ruledef_ext.md) - Extended rule definition retrieval

## Notes and Other Information
- Located at src/backend/utils/adt/ruleutils.c:575-656
- Static function (internal use only)
- Uses SPI for secure access control checking on pg_rewrite
- Returns NULL if rule is not found or accessible
- Implements plan caching for performance optimization
- Manages all memory allocation in the outer context to avoid SPI memory issues