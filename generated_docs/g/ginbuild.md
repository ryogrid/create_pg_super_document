# ginbuild

## Location
[src/backend/access/gin/gininsert.c:317-433](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gin/gininsert.c#L317-L433)

## Overview
The ginbuild function is the main entry point for building a GIN (Generalized Inverted Index) from scratch. It constructs the entire index by scanning the heap relation and inserting all tuples into the new index structure.

## Definition


## Detailed Description
The ginbuild function performs a complete build of a GIN index from an existing heap relation. It follows these key steps:

1. **Initialization**: Verifies the index is empty and initializes the GIN state and build statistics
2. **Meta and Root Page Setup**: Creates and initializes the meta page and root page of the index
3. **Memory Context Creation**: Sets up temporary memory contexts for build operations and user-defined function calls
4. **Heap Scan**: Performs a full table scan using table_index_build_scan with ginBuildCallback to process each tuple
5. **Entry Insertion**: Processes accumulated entries and inserts them into the index structure
6. **Finalization**: Updates statistics, handles WAL logging if required, and returns build results

The function uses a build accumulator (buildstate.accum) to collect entries during the heap scan before inserting them into the index, which helps optimize the build process.

## Parameters / Member Variables
- : The heap relation from which to build the index
- : The target GIN index relation being constructed  
- : Index metadata including column information and predicate details

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetNumberOfBlocks
  - [initGinState](../i/initGinState.md)
  - [GinNewBuffer](../G/GinNewBuffer.md)
  - [GinInitMetabuffer](../G/GinInitMetabuffer.md)
  - [GinInitBuffer](../G/GinInitBuffer.md)
  - [ginInitBA](ginInitBA.md)
  - [table_index_build_scan](../t/table_index_build_scan.md)
  - [ginBuildCallback](ginBuildCallback.md)
  - [ginBeginBAScan](ginBeginBAScan.md)
  - [ginGetBAEntry](ginGetBAEntry.md)
  - [ginEntryInsert](ginEntryInsert.md)
  - [ginUpdateStats](ginUpdateStats.md)
  - [log_newpage_range](../l/log_newpage_range.md)
- Called from (representative examples):
  - [ginhandler](ginhandler.md) (via access method handler)

## Notes and Other Information
- The function ensures the index is completely empty before starting the build process
- Uses two separate memory contexts: one for general build data and another specifically for user-defined function calls
- Disallows synchronized scans during the heap scan to maintain TID order preference
- Handles WAL logging by writing all pages at the end if WAL is required for the relation
- Returns an IndexBuildResult containing statistics about heap tuples processed and index tuples created
- The build process is interruptible via CHECK_FOR_INTERRUPTS() calls during entry insertion