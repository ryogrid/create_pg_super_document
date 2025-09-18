# spghandler

## Location
[src/backend/access/spgist/spgutils.c:44-114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgutils.c#L44-L114)

## Overview
The spghandler function is the main handler function for the SP-GiST (Space-Partitioned Generalized Search Tree) access method in PostgreSQL, returning an IndexAmRoutine structure with access method parameters and callback functions.

## Definition


## Detailed Description
The spghandler function creates and configures an IndexAmRoutine structure that defines the SP-GiST access method's capabilities and function pointers. It sets various boolean flags that describe the access method's features (such as whether it supports ordering, backward scans, unique indexes, etc.) and assigns callback functions for all SP-GiST operations including building, inserting, scanning, and maintenance operations.

The function is called by PostgreSQL's access method infrastructure to obtain the SP-GiST access method's interface. It configures the access method to support features like null searches, storage of different data types, include columns, and parallel vacuum operations, while disabling features like ordering, backward scans, and unique constraints.

## Parameters / Member Variables
- This function takes no explicit parameters (uses PG_FUNCTION_ARGS macro)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (to create IndexAmRoutine)
  - [spgbuild](spgbuild.md), spgbuildempty, spginsert
  - [spgbulkdelete](spgbulkdelete.md), spgvacuumcleanup
  - [spgcanreturn](spgcanreturn.md), spgcostestimate, spgoptions, spgproperty
  - [spgvalidate](spgvalidate.md), spgadjustmembers
  - [spgbeginscan](spgbeginscan.md), spgrescan, spggettuple, spggetbitmap, spgendscan
  - Constants: SPGISTNProc, SPGIST_OPTIONS_PROC, VACUUM_OPTION_PARALLEL_BULKDEL, VACUUM_OPTION_PARALLEL_COND_CLEANUP
- Called from (representative examples):
  - PostgreSQL access method infrastructure (no direct references found in indexed code)

## Notes and Other Information
- Located in src/backend/access/spgist/spgutils.c:44-114
- This is the entry point function that PostgreSQL calls to get the SP-GiST access method interface
- The function sets amcanorderbyop to true, indicating SP-GiST supports ORDER BY operations with operators
- Sets amsearchnulls to true, allowing searches for NULL values
- Enables parallel vacuum operations but disables parallel building and scanning
- The amcaninclude flag is set to true, supporting INCLUDE columns in SP-GiST indexes