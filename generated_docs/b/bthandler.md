# bthandler

## Location
[src/backend/access/nbtree/nbtree.c:101-158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/nbtree/nbtree.c#L101-L158)

## Overview
The bthandler function is the main access method handler for PostgreSQL B-tree indexes, returning a complete IndexAmRoutine structure with all access method parameters and callback functions.

## Definition

```c
struct metapage. */
	metabuf = smgr_bulk_get_buf(bulkstate);
```
## Detailed Description
The bthandler function serves as the central registry for B-tree index access method capabilities and operations. It constructs and returns an IndexAmRoutine structure that defines all the properties, capabilities, and function pointers that PostgreSQL's index access method framework needs to interact with B-tree indexes. This includes specifying what operations the B-tree access method supports (like ordering, uniqueness, backward scans), what callback functions to use for various operations (building, inserting, scanning), and configuration parameters that control the behavior of B-tree indexes.

The function sets up comprehensive access method properties including support for ordered access, unique constraints, multi-column indexes, backward scanning, parallel operations, and various optimization flags. It also assigns all the necessary callback functions for index lifecycle operations from building and maintenance to querying and cleanup.

## Parameters / Member Variables
This function takes no specific parameters (uses PG_FUNCTION_ARGS macro for PostgreSQL function interface).

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create IndexAmRoutine)
  - [IndexAmRoutine](../I/IndexAmRoutine.md) (structure type)
  - BTMaxStrategyNumber, BTNProcs, BTOPTIONS_PROC (constants)
  - [btbuild](btbuild.md), btbuildempty, btinsert (build/insert operations)
  - [btbulkdelete](btbulkdelete.md), btvacuumcleanup (maintenance operations)  
  - [btcanreturn](btcanreturn.md), btcostestimate, btoptions (utility functions)
  - [btproperty](btproperty.md), btbuildphasename, btvalidate, btadjustmembers (metadata functions)
  - [btbeginscan](btbeginscan.md), btrescan, btgettuple, btgetbitmap, btendscan (scan operations)
  - [btmarkpos](btmarkpos.md), btrestrpos (position management)
  - [btestimateparallelscan](btestimateparallelscan.md), btinitparallelscan, btparallelrescan (parallel operations)
- Called from (representative examples):
  - PostgreSQL's access method framework during index operations
  - System catalog lookups for B-tree access method

## Notes and Other Information
- This is the main entry point that registers the B-tree access method with PostgreSQL
- The returned IndexAmRoutine structure defines the complete interface contract for B-tree indexes
- Sets amcanparallel and amcanbuildparallel to true, indicating support for parallel index operations
- Configures vacuum options to support both parallel bulk delete and conditional cleanup
- The function is typically called by PostgreSQL's access method framework, not directly by user code
- All B-tree specific callback functions are registered here, making this the central hub for B-tree functionality

## Simplified Source

```c
Datum bthandler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

    // Set access method capabilities
    amroutine->amstrategies = BTMaxStrategyNumber;
    amroutine->amsupport = BTNProcs;
    amroutine->amoptsprocnum = BTOPTIONS_PROC;

    // Define what B-tree indexes can do
    amroutine->amcanorder = true;        // Supports ordered scans
    amroutine->amcanorderbyop = false;   // No ORDER BY operator support
    amroutine->amcanbackward = true;     // Supports backward scans
    amroutine->amcanunique = true;       // Supports unique constraints
    amroutine->amcanmulticol = true;     // Supports multi-column indexes
    amroutine->amoptionalkey = true;     // Can handle scans without keys
    amroutine->amsearcharray = true;     // Supports array searches
    amroutine->amsearchnulls = true;     // Can search for nulls
    amroutine->amstorage = false;        // No TOAST storage needed
    amroutine->amclusterable = true;     // Supports CLUSTER operation
    amroutine->ampredlocks = true;       // Uses predicate locks
    amroutine->amcanparallel = true;     // Supports parallel scans
    amroutine->amcanbuildparallel = true; // Supports parallel builds
    amroutine->amcaninclude = true;      // Supports INCLUDE columns
    amroutine->amusemaintenanceworkmem = false;
    amroutine->amsummarizing = false;

    // Parallel vacuum options
    amroutine->amparallelvacuumoptions =
        VACUUM_OPTION_PARALLEL_BULKDEL | VACUUM_OPTION_PARALLEL_COND_CLEANUP;

    amroutine->amkeytype = InvalidOid;

    // Assign all callback functions
    amroutine->ambuild = btbuild;
    amroutine->ambuildempty = btbuildempty;
    amroutine->aminsert = btinsert;
    amroutine->aminsertcleanup = NULL;
    amroutine->ambulkdelete = btbulkdelete;
    amroutine->amvacuumcleanup = btvacuumcleanup;
    amroutine->amcanreturn = btcanreturn;
    amroutine->amcostestimate = btcostestimate;
    amroutine->amoptions = btoptions;
    amroutine->amproperty = btproperty;
    amroutine->ambuildphasename = btbuildphasename;
    amroutine->amvalidate = btvalidate;
    amroutine->amadjustmembers = btadjustmembers;
    amroutine->ambeginscan = btbeginscan;
    amroutine->amrescan = btrescan;
    amroutine->amgettuple = btgettuple;
    amroutine->amgetbitmap = btgetbitmap;
    amroutine->amendscan = btendscan;
    amroutine->ammarkpos = btmarkpos;
    amroutine->amrestrpos = btrestrpos;
    amroutine->amestimateparallelscan = btestimateparallelscan;
    amroutine->aminitparallelscan = btinitparallelscan;
    amroutine->amparallelrescan = btparallelrescan;

    PG_RETURN_POINTER(amroutine);
}
```