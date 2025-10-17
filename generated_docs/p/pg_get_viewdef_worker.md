# pg_get_viewdef_worker

## Location
[src/backend/utils/adt/ruleutils.c:768-850](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L768-L850)

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
  - [initStringInfo](../i/initStringInfo.md)
  - [SPI_connect](../S/SPI_connect.md)
  - [SPI_prepare](../S/SPI_prepare.md)
  - [SPI_keepplan](../S/SPI_keepplan.md)
  - [SPI_execute_plan](../S/SPI_execute_plan.md)
  - [SPI_finish](../S/SPI_finish.md)
  - DirectFunctionCall1
  - [namein](../n/namein.md)
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

## Simplified Source

```c
static char *
pg_get_viewdef_worker(Oid viewoid, int prettyFlags, int wrapColumn)
{
    Datum args[2];
    char nulls[2];
    HeapTuple ruletup;
    TupleDesc rulettc;
    StringInfoData buf;

    // Initialize result buffer in outer context
    initStringInfo(&buf);

    // Connect to SPI for secure catalog access
    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

    // Prepare query plan on first call (cached)
    if (plan_getviewrule == NULL) {
        Oid argtypes[2] = {OIDOID, NAMEOID};
        SPIPlanPtr plan = SPI_prepare(query_getviewrule, 2, argtypes);
        if (plan == NULL)
            elog(ERROR, "SPI_prepare failed");
        SPI_keepplan(plan);
        plan_getviewrule = plan;
    }

    // Execute query to get view's SELECT rule
    args[0] = ObjectIdGetDatum(viewoid);
    args[1] = DirectFunctionCall1(namein, CStringGetDatum(ViewSelectRuleName));
    nulls[0] = nulls[1] = ' ';
    int spirc = SPI_execute_plan(plan_getviewrule, args, nulls, true, 0);

    if (spirc != SPI_OK_SELECT)
        elog(ERROR, "failed to get pg_rewrite tuple for view %u", viewoid);

    // Format view definition if found
    if (SPI_processed == 1) {
        ruletup = SPI_tuptable->vals[0];
        rulettc = SPI_tuptable->tupdesc;
        make_viewdef(&buf, ruletup, rulettc, prettyFlags, wrapColumn);
    }

    // Clean up SPI connection
    if (SPI_finish() != SPI_OK_FINISH)
        elog(ERROR, "SPI_finish failed");

    return (buf.len == 0) ? NULL : buf.data;
}
```