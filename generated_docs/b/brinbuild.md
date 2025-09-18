# brinbuild

## Location
[src/backend/access/brin/brin.c:1095-1263](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L1095-L1263)

## Overview
The main function responsible for building a new BRIN (Block Range Index) from scratch, including metadata initialization, tuple scanning, and optional parallel processing.

## Definition


## Detailed Description
 is the primary index construction function for BRIN indexes. It performs a complete build of a new BRIN index by:

1. **Initialization Phase**: Creates and initializes the metadata page with version and pages-per-range information
2. **WAL Logging**: Records the index creation in write-ahead log if needed for crash recovery
3. **State Setup**: Initializes build state including revmap (reverse mapping) and tuple processing structures
4. **Parallel Processing**: Optionally launches parallel workers to scan different portions of the heap table
5. **Data Processing**: Either merges results from parallel workers or performs serial table scan using 
6. **Range Completion**: Fills empty ranges and finalizes index construction
7. **Cleanup**: Releases resources and returns build statistics

The function supports both serial and parallel index building modes, with parallel mode being chosen based on the  setting in .

## Parameters / Member Variables
- : The heap relation (table) being indexed
- : The BRIN index relation being built
- : Contains index metadata including parallel worker configuration and concurrent build settings

## Dependencies
- Functions called/Symbols referenced:
  - : Get table size in blocks
  - : Extend relation with new block
  - : Initialize BRIN metadata page
  - : Get pages per range setting
  - : Initialize reverse mapping structure
  - : Set up build state
  - : Start parallel index build
  - : Merge parallel worker results
  - : Perform table scan for index building
  - : Process each tuple during scan
  - : Create and insert index tuples
  - : Fill ranges with no data
  - : Clean up reverse mapping
  - : Clean up build state
- Called from (representative examples):
  - : BRIN access method handler function

## Notes and Other Information
- Expects to be called on an empty index relation (throws error if blocks exist)
- Critical sections not required as relation creation rollback handles errors
- Parallel building requires sufficient  (32MB per worker by default)
- Uses physical order scanning (no syncscan) to ensure proper range generation from block 0
- Supports concurrent index builds when  is set
- WAL logging ensures crash recovery consistency for permanent indexes
- Returns  with heap tuple count and index tuple count statistics