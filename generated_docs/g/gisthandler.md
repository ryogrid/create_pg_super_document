# gisthandler

## Location
[src/backend/access/gist/gist.c:59-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gist.c#L59-L121)

## Overview
The gisthandler function is the main entry point for the GiST (Generalized Search Tree) access method in PostgreSQL, returning an IndexAmRoutine structure populated with all the access method parameters and callback functions.

## Definition

```c
Datum
gisthandler(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the handler for the GiST access method, creating and configuring an IndexAmRoutine structure that defines all the capabilities and function pointers for GiST operations. It sets up the access method's properties such as whether it supports ordering, uniqueness, multi-column indexes, and various other characteristics. The function also assigns all the callback functions that implement the actual GiST operations like building, inserting, scanning, and maintenance.

The function configures the GiST access method with specific capabilities:
- Supports order-by operations but not regular ordering
- Allows multi-column indexes and optional keys
- Supports storage of compressed data and clustering
- Enables predicate locking but disables parallel operations
- Allows include columns but doesn't use maintenance work memory

## Parameters / Member Variables
This function takes no specific parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface).

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates IndexAmRoutine node)
  - [gistbuild](gistbuild.md) (index building function)
  - [gistbuildempty](gistbuildempty.md) (empty index building)
  - [gistinsert](gistinsert.md) (tuple insertion)
  - [gistbulkdelete](gistbulkdelete.md) (bulk deletion)
  - [gistvacuumcleanup](gistvacuumcleanup.md) (vacuum cleanup)
  - [gistcanreturn](gistcanreturn.md) (index-only scan support)
  - [gistcostestimate](gistcostestimate.md) (cost estimation)
  - [gistoptions](gistoptions.md) (index options handling)
  - [gistproperty](gistproperty.md) (property queries)
  - [gistvalidate](gistvalidate.md) (validation function)
  - [gistadjustmembers](gistadjustmembers.md) (operator class adjustment)
  - [gistbeginscan](gistbeginscan.md) (scan initialization)
  - [gistrescan](gistrescan.md) (scan restart)
  - [gistgettuple](gistgettuple.md) (tuple retrieval)
  - [gistgetbitmap](gistgetbitmap.md) (bitmap scan)
  - [gistendscan](gistendscan.md) (scan cleanup)
- Constants used:
  - GISTNProcs (number of support procedures)
  - GIST_OPTIONS_PROC (options procedure number)
  - VACUUM_OPTION_PARALLEL_BULKDEL
  - VACUUM_OPTION_PARALLEL_COND_CLEANUP
- Called from:
  - PostgreSQL access method system (no direct references found in codebase)

## Notes and Other Information
- This is a PostgreSQL system function that integrates GiST into the database's access method framework
- The function is typically called by the PostgreSQL system when GiST indexes are created or used
- All parallel operations are disabled for GiST (amcanparallel = false, amcanbuildparallel = false)
- The access method supports include columns (amcaninclude = true) which allows non-key columns in the index
- Located in src/backend/access/gist/gist.c:59-121

## Simplified Source

```c
Datum
gisthandler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

    // Configure access method capabilities
    amroutine->amstrategies = 0;           // No strategy numbers used
    amroutine->amsupport = GISTNProcs;     // Number of support procedures
    amroutine->amoptsprocnum = GIST_OPTIONS_PROC;

    // Set capability flags
    amroutine->amcanorder = false;         // No ordering support
    amroutine->amcanorderbyop = true;      // Supports ORDER BY operators
    amroutine->amcanbackward = false;      // No backward scans
    amroutine->amcanunique = false;        // No unique constraints
    amroutine->amcanmulticol = true;       // Multi-column indexes supported
    amroutine->amoptionalkey = true;       // Optional scan keys allowed
    amroutine->amsearcharray = false;      // No array search
    amroutine->amsearchnulls = true;       // Can search NULL values
    amroutine->amstorage = true;           // Supports storage compression
    amroutine->amclusterable = true;       // Supports clustering
    amroutine->ampredlocks = true;         // Uses predicate locks
    amroutine->amcanparallel = false;      // No parallel scans
    amroutine->amcanbuildparallel = false; // No parallel builds
    amroutine->amcaninclude = true;        // Supports include columns

    // Set callback functions for GiST operations
    amroutine->ambuild = gistbuild;
    amroutine->ambuildempty = gistbuildempty;
    amroutine->aminsert = gistinsert;
    amroutine->ambulkdelete = gistbulkdelete;
    amroutine->amvacuumcleanup = gistvacuumcleanup;
    amroutine->amcanreturn = gistcanreturn;
    amroutine->amcostestimate = gistcostestimate;
    amroutine->amoptions = gistoptions;
    amroutine->amproperty = gistproperty;
    amroutine->amvalidate = gistvalidate;
    amroutine->amadjustmembers = gistadjustmembers;
    amroutine->ambeginscan = gistbeginscan;
    amroutine->amrescan = gistrescan;
    amroutine->amgettuple = gistgettuple;
    amroutine->amgetbitmap = gistgetbitmap;
    amroutine->amendscan = gistendscan;

    // NULL callbacks for unsupported operations
    amroutine->aminsertcleanup = NULL;
    amroutine->ammarkpos = NULL;
    amroutine->amrestrpos = NULL;
    amroutine->amestimateparallelscan = NULL;
    amroutine->aminitparallelscan = NULL;
    amroutine->amparallelrescan = NULL;

    PG_RETURN_POINTER(amroutine);
}
```