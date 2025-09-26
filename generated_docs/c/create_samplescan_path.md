# create_samplescan_path

## Location
[src/backend/optimizer/util/pathnode.c:952-992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L952-L992)

## Overview
Creates a Path node representing a sample scan access method for a relation using TABLESAMPLE functionality, providing a random subset of rows from the table.

## Definition

```c
Path *
create_samplescan_path(PlannerInfo *root, RelOptInfo *rel, Relids required_outer)
```
## Detailed Description
The  function constructs a Path node specifically for sample scan operations. A sample scan implements the SQL TABLESAMPLE clause, which reads a statistically representative sample of rows from a relation rather than the complete dataset. This is useful for approximate queries, statistical analysis, or quick estimates on large tables.

Key characteristics of the created path:
- Sets pathtype to T_SampleScan to identify the sampling access method
- Uses the relation's reltarget as the path target
- Handles parameterization through get_baserel_parampathinfo
- Always non-parallel (parallel_aware = false, parallel_workers = 0)
- Produces unordered results (pathkeys = NIL)
- Inherits parallel safety from the relation's consider_parallel flag

The function delegates cost calculation to , which accounts for the reduced I/O and processing overhead compared to full table scans.

## Parameters / Member Variables
- : The PlannerInfo structure containing global planning context and information
- : The RelOptInfo structure representing the relation to be sampled  
- : Set of outer relations whose parameters are needed by this path (for parameterized paths)

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates new Path node)
  - [get_baserel_parampathinfo](../g/get_baserel_parampathinfo.md) (handles parameterization)
  - [cost_samplescan](cost_samplescan.md) (calculates sampling costs)
  - T_SampleScan (pathtype constant)
  - NIL (empty list constant)

- Called from (representative examples):
  - [set_tablesample_rel_pathlist](../s/set_tablesample_rel_pathlist.md)
  - [reparameterize_path](../r/reparameterize_path.md)

## Notes and Other Information
- Sample scans are always non-parallel since sampling coordination between workers is complex
- Like sequential scans, sample scans produce unordered results (pathkeys = NIL)
- The sampling method and parameters are determined by the TABLESAMPLE clause in the query
- Cost calculation considers the sampling percentage and method overhead
- Sample scans are typically much cheaper than full table scans for large relations
- The function creates a basic Path node rather than a specialized sampling-specific subclass