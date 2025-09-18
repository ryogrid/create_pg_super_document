# IndexVacuumInfo

## Location
src/include/access/genam.h: 44 - 54

## Overview
IndexVacuumInfo is a structure that contains input arguments passed to ambulkdelete and amvacuumcleanup functions during index vacuum and cleanup operations.

## Definition


## Detailed Description
IndexVacuumInfo serves as a parameter structure for index access method vacuum operations. It provides all necessary context and configuration for both bulk delete operations (ambulkdelete) and vacuum cleanup operations (amvacuumcleanup). The structure encapsulates information about the target index, its corresponding heap relation, operational modes, progress reporting settings, and access strategies.

A key aspect of this structure is the handling of tuple count estimates. The num_heap_tuples field is accurate only when estimated_count is false. When estimated_count is true, the value represents an estimate (typically from pg_class.reltuples) and may even be -1. During ambulkdelete operations, this will always be an estimate.

## Parameters / Member Variables
- : The index relation being vacuumed
- : The heap relation that the index belongs to  
- : Boolean flag indicating this is an ANALYZE operation without actual vacuuming
- : Boolean flag to enable progress reporting via the progress.h mechanism
- : Boolean flag indicating whether num_heap_tuples is an estimate or accurate count
- : The ereport level to use for progress messages (e.g., DEBUG1, LOG, etc.)
- : The number of tuples remaining in the heap (may be estimated)
- : Buffer access strategy to use for reading pages during vacuum operations

## Dependencies
- Functions called/Symbols referenced:
  - BufferAccessStrategy (for memory management strategy)
- Called from (representative examples):
  - brinbulkdelete/brinvacuumcleanup (BRIN index vacuum)
  - ginbulkdelete/ginvacuumcleanup (GIN index vacuum)
  - gistbulkdelete/gistvacuumcleanup (GiST index vacuum)
  - hashbulkdelete/hashvacuumcleanup (Hash index vacuum)
  - btbulkdelete/btvacuumcleanup (B-tree index vacuum)
  - spgbulkdelete/spgvacuumcleanup (SP-GiST index vacuum)
  - index_bulk_delete/index_vacuum_cleanup (generic index vacuum functions)

## Notes and Other Information
- This structure is defined in src/include/access/genam.h and is used across all index access methods
- The estimated_count flag is crucial for understanding the reliability of the num_heap_tuples value
- Progress reporting can be controlled through the report_progress flag and message_level setting
- The BufferAccessStrategy helps optimize I/O patterns during vacuum operations
- Used in both parallel and non-parallel vacuum operations
- The analyze_only flag allows the same structure to be used for ANALYZE operations that don't perform actual vacuuming