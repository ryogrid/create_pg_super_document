# set_base_rel_pathlists

## Location
[src/backend/optimizer/path/allpaths.c:333-359](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L333-L359)

## Overview
Finds all available paths for scanning each base-relation entry, including sequential scan and available indices, attaching useful paths to each relation's pathlist field.

## Definition

```c
static void
set_base_rel_pathlists(PlannerInfo *root)
```
## Detailed Description
This function performs the critical task of generating access paths for each base relation in the query. It iterates through all base relations and delegates to set_rel_pathlist() to discover and create all viable access methods for each table. This includes sequential scans, index scans, and other specialized access methods based on the relation's characteristics and available indices.

The function operates after size estimates have been established, ensuring that path costing can be performed accurately. It processes only base relations (RELOPT_BASEREL), filtering out other relation types that don't require individual access path generation.

## Parameters / Member Variables
- : PlannerInfo structure containing global optimizer state, including the simple_rel_array and simple_rte_array

## Dependencies
- Functions called/Symbols referenced:
  - RELOPT_BASEREL (enum value)
  - [set_rel_pathlist](set_rel_pathlist.md)
- Called from (representative examples):
  - [make_one_rel](../m/make_one_rel.md)

## Notes and Other Information
- Located in src/backend/optimizer/path/allpaths.c:333-359
- Static function, only used within the allpaths.c module
- Operates after set_base_rel_sizes() has established size estimates for proper path costing
- Delegates the actual path generation work to set_rel_pathlist() for each valid base relation
- Essential step in the query optimization pipeline, bridging size estimation and join path generation
- Includes standard safeguards to skip empty array slots and non-baserel RTEs
- The generated paths become the foundation for subsequent join path exploration