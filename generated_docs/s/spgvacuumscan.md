# spgvacuumscan

## Location
src/backend/access/spgist/spgvacuum.c: 804 - 915

## Overview
Performs the main bulk delete scan for SP-GiST vacuum operations, coordinating page-by-page processing and pending list management.

## Definition


## Detailed Description
The `spgvacuumscan` function orchestrates the complete bulk delete scan process for SP-GiST indexes. It implements a two-phase approach:

1. **Main scan phase**: Iterates through all index pages in physical order (excluding metapage), calling `spgvacuumpage` for each page and processing the pending list after each page with `spgprocesspending`.

2. **Finalization phase**: Updates the metapage with cached information, vacuums the Free Space Map (FSM) if pages were deleted, and reports final statistics.

The function uses a dynamic approach to handle concurrent page additions by repeatedly checking the relation length during scanning. It includes provisions for relation extension locking when needed and handles both local and shared relations appropriately.

The scan ensures all leaf pages are visited, which is critical for correctness since deletable tuples might exist on pages added during the scan. It also includes commented-out truncation logic that is disabled due to concurrency concerns.

## Parameters / Member Variables
- `bds`: Bulk delete state containing index relation, statistics, SP-GiST state, pending list, and transaction information

## Dependencies
- Functions called/Symbols referenced:
  - initSpGistState
  - GetActiveSnapshot
  - LockRelationForExtension/UnlockRelationForExtension
  - RelationGetNumberOfBlocks
  - spgvacuumpage
  - spgprocesspending
  - SpGistUpdateMetaPage
  - IndexFreeSpaceMapVacuum
  - RELATION_IS_LOCAL
- Called from (representative examples):
  - spgbulkdelete
  - spgvacuumcleanup

## Notes and Other Information
- Initializes bulk delete statistics and resets counters for multiple scans within a single VACUUM command
- Uses physical page order scanning for better I/O performance with kernel read-ahead
- Handles concurrent relation extension by dynamically checking relation length
- Includes disabled truncation logic due to safety concerns with concurrent operations
- Updates FSM only when empty pages are found to optimize performance
- Critical for maintaining SP-GiST index consistency during vacuum operations
- Similar in concept to btree's btvacuumscan but adapted for SP-GiST's tree structure