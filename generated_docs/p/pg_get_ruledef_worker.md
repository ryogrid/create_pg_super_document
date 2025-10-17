# pg_get_ruledef_worker

## Location
[src/backend/utils/adt/ruleutils.c:575-656](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L575-L656)

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
  - `[SPI_connect](../S/SPI_connect.md)/SPI_finish` - Database connection management
  - `[SPI_prepare](../S/SPI_prepare.md)/SPI_keepplan` - Prepared statement management
  - [SPI_execute_plan](../S/SPI_execute_plan.md) - [Query](../Q/Query.md) execution
  - [make_ruledef](../m/make_ruledef.md) - Core rule formatting function
  - `[initStringInfo](../i/initStringInfo.md)` - [String](../S/String.md) buffer initialization
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

## Simplified Source

```c
static char *
pg_get_ruledef_worker(Oid ruleoid, int prettyFlags)
{
    Datum args[1];
    char nulls[1];
    HeapTuple ruletup;
    TupleDesc rulettc;
    StringInfoData buf;

    // Initialize result buffer in outer context
    initStringInfo(&buf);

    // Connect to SPI for secure catalog access
    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

    // Prepare query plan on first call (cached)
    if (plan_getrulebyoid == NULL) {
        Oid argtypes[1] = {OIDOID};
        SPIPlanPtr plan = SPI_prepare(query_getrulebyoid, 1, argtypes);
        if (plan == NULL)
            elog(ERROR, "SPI_prepare failed");
        SPI_keepplan(plan);
        plan_getrulebyoid = plan;
    }

    // Execute query to get rule tuple
    args[0] = ObjectIdGetDatum(ruleoid);
    nulls[0] = ' ';
    int spirc = SPI_execute_plan(plan_getrulebyoid, args, nulls, true, 0);

    if (spirc != SPI_OK_SELECT)
        elog(ERROR, "failed to get pg_rewrite tuple for rule %u", ruleoid);

    // Format rule definition if found
    if (SPI_processed == 1) {
        ruletup = SPI_tuptable->vals[0];
        rulettc = SPI_tuptable->tupdesc;
        make_ruledef(&buf, ruletup, rulettc, prettyFlags);
    }

    // Clean up SPI connection
    if (SPI_finish() != SPI_OK_FINISH)
        elog(ERROR, "SPI_finish failed");

    return (buf.len == 0) ? NULL : buf.data;
}
```