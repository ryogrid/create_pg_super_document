# pg_cursor

## Location
[src/backend/utils/mmgr/portalmem.c:1131-1170](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/mmgr/portalmem.c#L1131-L1170)

## Overview
SQL-callable function that returns information about all available cursors (portals) in the current session.

## Definition

```c
Datum
pg_cursor(PG_FUNCTION_ARGS)
```
## Detailed Description
pg_cursor is a system function that provides information about all visible cursors (portals) currently active in the PostgreSQL session. This function is designed to be called from SQL and returns a set of rows containing details about each cursor, including its name, source text, options, and creation time.

The function scans through the global PortalHashTable and collects information about portals that meet certain visibility criteria:
1. The portal must be marked as visible (portal->visible == true)
2. The portal must have source text available (indicating PortalDefineQuery has been called)

For each qualifying portal, the function returns a tuple containing six pieces of information: cursor name, source SQL text, and three boolean flags indicating cursor options (HOLD, BINARY, SCROLL), plus the creation timestamp.

The function uses PostgreSQL's set-returning function (SRF) infrastructure to return multiple rows, materializing all results in a tuplestore during a single scan to avoid consistency issues.

## Parameters
- : Standard PostgreSQL function arguments macro, providing access to function call context including result info

## Dependencies
- Functions called/Symbols referenced:
  - [InitMaterializedSRF](../I/InitMaterializedSRF.md)
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - CStringGetTextDatum
  - [BoolGetDatum](../B/BoolGetDatum.md)
  - [TimestampTzGetDatum](../T/TimestampTzGetDatum.md)
  - [tuplestore_putvalues](../t/tuplestore_putvalues.md)
- Data types used:
  - [ReturnSetInfo](../R/ReturnSetInfo.md)
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md)
  - [PortalHashEnt](../P/PortalHashEnt.md)
  - [Portal](../P/Portal.md)
  - Datum
- Constants used:
  - CURSOR_OPT_HOLD
  - CURSOR_OPT_BINARY
  - CURSOR_OPT_SCROLL

## Notes and Other Information
- This function is typically exposed as a system view or function that users can query to see active cursors
- The function only reports 'visible' portals, filtering out internal or temporary portals
- The tuplestore approach ensures consistent results even if the portal hash table changes during execution
- The function returns 6 columns: name, statement, is_holdable, is_binary, is_scrollable, creation_time
- Portals without source text are excluded, which filters out incomplete or internal portals
- The function is defined in src/backend/utils/mmgr/portalmem.c:1131-1170

## Simplified Source

```c
Datum
pg_cursor(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    HASH_SEQ_STATUS hash_seq;
    PortalHashEnt *hentry;

    // Initialize result set for returning multiple rows
    InitMaterializedSRF(fcinfo, 0);

    // Scan through all portals in the hash table
    hash_seq_init(&hash_seq, PortalHashTable);
    while ((hentry = hash_seq_search(&hash_seq)) != NULL)
    {
        Portal portal = hentry->portal;
        Datum values[6];
        bool nulls[6] = {0};

        // Skip invisible portals and those without source text
        if (!portal->visible || !portal->sourceText)
            continue;

        // Build result tuple with cursor information
        values[0] = CStringGetTextDatum(portal->name);
        values[1] = CStringGetTextDatum(portal->sourceText);
        values[2] = BoolGetDatum(portal->cursorOptions & CURSOR_OPT_HOLD);
        values[3] = BoolGetDatum(portal->cursorOptions & CURSOR_OPT_BINARY);
        values[4] = BoolGetDatum(portal->cursorOptions & CURSOR_OPT_SCROLL);
        values[5] = TimestampTzGetDatum(portal->creation_time);

        tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
    }

    return (Datum) 0;
}
```