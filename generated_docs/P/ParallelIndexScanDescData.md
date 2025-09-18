# ParallelIndexScanDescData

## Location
src/include/access/relscan.h: 170 - 176

## Overview
ParallelIndexScanDescData is a structure that contains shared memory information for parallel index scans, enabling multiple worker processes to coordinate access to the same index during parallel query execution.

## Definition


## Detailed Description
This structure serves as the foundation for parallel index scanning in PostgreSQL. It resides in shared memory and allows multiple worker processes to perform coordinated scans of the same index. The structure contains essential metadata about the relation and index being scanned, along with snapshot information and a flexible array member that can accommodate access method-specific data.

The structure is designed to be extensible - different index access methods (B-tree, hash, GiST, etc.) can append their own specific parallel scan state after the base structure using the ps_offset field to locate their data within the shared memory segment.

## Parameters / Member Variables
- : Object identifier (OID) of the heap relation being scanned
- : Object identifier (OID) of the index relation being used for the scan
- : Byte offset within the structure where access method-specific parallel scan data begins
- : Flexible array member containing serialized snapshot data that defines transaction visibility for all participating workers

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - [index_parallelscan_estimate](../i/index_parallelscan_estimate.md)
  - [index_parallelscan_initialize](../i/index_parallelscan_initialize.md)
  - [ParallelIndexScanDesc](ParallelIndexScanDesc.md) (typedef pointer)
  - [IndexScanDescData](../I/IndexScanDescData.md) (contains pointer to this structure)

## Notes and Other Information
- The structure uses a flexible array member pattern to accommodate variable-length snapshot data and access method-specific information
- Memory allocation is calculated by index_parallelscan_estimate() which accounts for both the base structure size and any AM-specific data requirements
- The ps_offset field enables access methods to store their parallel scan state immediately after the snapshot data in a type-safe manner
- This structure is part of PostgreSQL's parallel query execution framework and is essential for distributing index scan work across multiple processes
- The snapshot data ensures all worker processes see a consistent view of the database during the parallel scan operation