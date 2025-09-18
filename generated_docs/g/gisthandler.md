# gisthandler

## Location
src/backend/access/gist/gist.c: 59 - 121

## Overview
The gisthandler function is the main entry point for the GiST (Generalized Search Tree) access method in PostgreSQL, returning an IndexAmRoutine structure populated with all the access method parameters and callback functions.

## Definition


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
  - gistbuild (index building function)
  - gistbuildempty (empty index building)
  - gistinsert (tuple insertion)
  - gistbulkdelete (bulk deletion)
  - gistvacuumcleanup (vacuum cleanup)
  - gistcanreturn (index-only scan support)
  - gistcostestimate (cost estimation)
  - gistoptions (index options handling)
  - gistproperty (property queries)
  - gistvalidate (validation function)
  - gistadjustmembers (operator class adjustment)
  - gistbeginscan (scan initialization)
  - gistrescan (scan restart)
  - gistgettuple (tuple retrieval)
  - gistgetbitmap (bitmap scan)
  - gistendscan (scan cleanup)
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