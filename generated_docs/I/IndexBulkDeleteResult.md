# IndexBulkDeleteResult

## Location
src/include/access/genam.h: 75 - 84

## Overview
IndexBulkDeleteResult is a structure that holds statistics returned by ambulkdelete and amvacuumcleanup functions, providing comprehensive information about the vacuum operation's impact on the index.

## Definition


## Detailed Description
IndexBulkDeleteResult serves as the return structure for index vacuum operations, providing comprehensive statistics about the state of the index before and after vacuum operations. This structure is typically allocated by the first ambulkdelete call and then passed through subsequent calls until reaching amvacuumcleanup. However, amvacuumcleanup must be prepared to allocate it when no ambulkdelete calls were made (because no tuples needed deletion).

Index access methods can extend this structure by returning a larger struct where IndexBulkDeleteResult is the first field, allowing ambulkdelete to communicate additional private data to amvacuumcleanup.

The structure distinguishes between different types of page statistics: pages_newly_deleted tracks pages deleted by the current vacuum operation, while pages_deleted and pages_free refer to free space within the index file generally.

## Parameters / Member Variables
- : The total number of pages remaining in the index after vacuum operations
- : Boolean flag indicating whether num_index_tuples is an accurate count or an estimate
- : The number of tuples remaining in the index (may be estimated based on num_heap_tuples)
- : The number of tuples that were removed during the current vacuum operation
- : The number of pages that were marked as deleted by the current vacuum operation
- : The total number of pages marked as deleted (including those deleted by previous operations)
- : The number of pages that are available for reuse

## Dependencies
- Functions called/Symbols referenced: None (simple data structure)
- Called from (representative examples):
  - [brinbulkdelete](../b/brinbulkdelete.md)/brinvacuumcleanup (BRIN index vacuum)
  - [ginbulkdelete](../g/ginbulkdelete.md)/ginvacuumcleanup (GIN index vacuum)
  - [gistbulkdelete](../g/gistbulkdelete.md)/gistvacuumcleanup (GiST index vacuum)
  - [hashbulkdelete](../h/hashbulkdelete.md)/hashvacuumcleanup (Hash index vacuum)
  - [btbulkdelete](../b/btbulkdelete.md)/btvacuumcleanup (B-tree index vacuum)
  - [spgbulkdelete](../s/spgbulkdelete.md)/spgvacuumcleanup (SP-GiST index vacuum)
  - [index_bulk_delete](../i/index_bulk_delete.md)/index_vacuum_cleanup (generic index vacuum functions)

## Notes and Other Information
- This structure is defined in src/include/access/genam.h and is used across all index access methods
- The estimated_count field should be copied from IndexVacuumInfo when index AMs compute num_index_tuples by reference to num_heap_tuples
- The distinction between pages_newly_deleted, pages_deleted, and pages_free helps track vacuum effectiveness and index space utilization
- Used extensively in both serial and parallel vacuum operations
- The structure can be extended by specific index access methods to include additional private data
- Essential for updating relation statistics (pg_class.reltuples, pg_class.relpages) after vacuum operations
- Provides crucial data for vacuum progress reporting and determining when additional vacuum cycles may be needed