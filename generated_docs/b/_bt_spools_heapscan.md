# _bt_spools_heapscan

## Location
src/backend/access/nbtree/nbtsort.c: 363 - 514

## Overview
Manages the heap scanning phase of B-tree index construction, creating spool structures for temporary storage and coordinating parallel processing when applicable.

## Definition


## Detailed Description
 is a critical function in B-tree index construction that handles the heap scanning phase. It performs several key operations:

1. **Spool Initialization**: Creates one or two BTSpool structures for temporary storage of index tuples during the build process. The primary spool handles live tuples, while an optional secondary spool manages dead tuples for unique indexes.

2. **Memory Management**: Allocates sort areas using  for the primary spool to optimize index creation performance. The secondary spool (if needed) uses the smaller  allocation since dead tuples are expected to be fewer.

3. **Parallel Processing Setup**: Detects and coordinates parallel index building when multiple worker processes are available. Sets up shared sort coordination structures for parallel tuplesort operations.

4. **Tuplesort Initialization**: Creates tuplesort states for both primary and secondary spools, configuring them for B-tree index tuple sorting with appropriate uniqueness and null handling settings.

5. **Heap Scanning**: Executes either serial or parallel heap scanning to read tuples from the source relation and populate the spool structures. Uses callback functions to process each tuple and add it to the appropriate spool.

6. **Progress Reporting**: Updates progress statistics for monitoring the index creation process, including tuple counts and scan progress.

7. **Cleanup Optimization**: Removes the secondary spool if no dead tuples were encountered during scanning, optimizing resource usage.

The function encapsulates all aspects of parallelism management, allowing the caller to simply call  when finished.

## Parameters
- : The source heap relation to scan for index tuples
- : The target index relation being constructed  
- : Build state structure to store spool references and build metadata
- : Index metadata including uniqueness, parallel worker count, and other properties

## Dependencies
- Functions called/Symbols referenced:
  -  - Initiates parallel processing setup
  -  - Creates tuplesort states for spools
  -  - Performs serial heap scanning
  -  - Performs parallel heap scanning  
  -  - Processes individual tuples during scan
  -  - Cleans up unused secondary spool
  - ,  - Progress reporting
  - , , ,  - Data structures
- Called from:
  -  - Main index construction function

## Notes and Other Information
- Uses  for primary spool allocation to speed index creation, while secondary spool uses smaller 
- Automatically detects when parallel processing is beneficial and coordinates multiple worker processes
- For unique indexes, maintains separate spools for live and dead tuples to optimize uniqueness checking
- Implements sophisticated memory management to ensure  represents an absolute high watermark regardless of parallelism
- Progress reporting integration allows monitoring of long-running index builds
- Returns the total number of heap tuples scanned for statistics and validation purposes