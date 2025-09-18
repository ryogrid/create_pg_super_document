# AnlIndexData

## Location
src/backend/commands/analyze.c: 63 - 69

## Overview
AnlIndexData is a structure that holds per-index data used during the ANALYZE command execution in PostgreSQL, containing information needed for analyzing index statistics.

## Definition


## Detailed Description
AnlIndexData serves as a container for index-specific information during ANALYZE operations. This structure is used to collect and organize data necessary for computing statistics on indexes, including both complete and partial indexes. It maintains references to the index metadata, statistical analysis structures for individual attributes, and fraction information for partial indexes that only cover a subset of table rows.

The structure is primarily used within the analyze.c module to coordinate the collection of index statistics, ensuring that the ANALYZE command can properly evaluate index usage patterns and data distribution for query planning optimization.

## Parameters / Member Variables
- : Pointer to IndexInfo structure containing the result from BuildIndexInfo, which holds metadata about the index structure and properties
- : Double precision value representing the fraction of table rows that are covered by this index (important for partial indexes)
- : Array of pointers to VacAttrStats structures, one for each index attribute that needs statistical analysis
- : Integer count of the number of attributes in the index that require analysis

## Dependencies
- Functions called/Symbols referenced:
  - IndexInfo
  - VacAttrStats
- Called from (representative examples):
  - [do_analyze_rel](../d/do_analyze_rel.md) (multiple references for index processing)
  - [compute_index_stats](../c/compute_index_stats.md) (for statistical computation)

## Notes and Other Information
- This structure is defined at src/backend/commands/analyze.c:63-69
- Used extensively in the do_analyze_rel function for managing multiple indexes during table analysis
- The tupleFract member is particularly important for partial indexes where statistics need to be weighted appropriately
- The structure facilitates parallel processing of multiple indexes by organizing per-index data separately
- Memory management for the vacattrstats array and its contents is handled by the calling functions