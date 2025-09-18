# reparameterize_path

## Location
[src/backend/optimizer/util/pathnode.c:3949-4114](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L3949-L4114)

## Overview
Attempts to modify a Path to have greater parameterization, primarily used to bring child paths of an appendrel to the same parameterization level for consistent join qual enforcement.

## Definition


## Detailed Description
This function creates a new path with increased parameterization from an existing path. It's primarily used in the context of append relations where all child paths need to enforce the same set of join quals. The function can only increase parameterization, not decrease it.

The function supports several path types including sequential scans, index scans, bitmap heap scans, subquery scans, result scans, append paths, material paths, and memoize paths. For unsupported path types, it returns NULL.

For index-based scans, the function performs optimizations by copying the existing path structure and updating only the parameterization info and costs, avoiding expensive recomputation of index conditions. For composite paths like append paths, it recursively reparameterizes all child paths.

The function intentionally does not pass created paths to add_path() since these specialized paths are designed for specific use cases (like append path members) rather than general-purpose optimization.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning information
- : The original path to be reparameterized
- : RelIds representing the required outer relation parameterization
- : Expected number of times this path will be executed in nested loops

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_subset](../b/bms_is_subset.md) (checks if parameterization can be increased)
  - PATH_REQ_OUTER (extracts required outer relations from path)
  - [create_seqscan_path](../c/create_seqscan_path.md), create_samplescan_path, create_bitmap_heap_path (path creation functions)
  - get_baserel_parampathinfo (retrieves parameterization info for base relations)
  - [cost_index](../c/cost_index.md) (recalculates index scan costs)
  - [create_subqueryscan_path](../c/create_subqueryscan_path.md), create_resultscan_path, create_append_path (specialized path creators)
  - [create_material_path](../c/create_material_path.md), create_memoize_path (utility path creators)
- Called from (representative examples):
  - [get_cheapest_parameterized_child_path](../g/get_cheapest_parameterized_child_path.md) (src/backend/optimizer/path/allpaths.c:2047)
  - [reparameterize_path](reparameterize_path.md) (recursive calls for composite paths)

## Notes and Other Information
- Can only increase parameterization, never decrease it
- Returns NULL for unsupported path types or when reparameterization fails
- Recursively processes composite paths like AppendPath, MaterialPath, and MemoizePath
- Optimized for IndexPath by avoiding recomputation of index conditions
- Created paths are not added to the general path list as they serve specific purposes
- Critical for ensuring consistent parameterization across child paths in append relations
- Supports parallel path processing by maintaining separation between regular and partial paths in append scenarios