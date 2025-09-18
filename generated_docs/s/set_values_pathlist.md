# set_values_pathlist

## Location
src/backend/optimizer/path/allpaths.c: 2816 - 2835

## Overview
Builds the single access path for a VALUES RTE (Range Table Entry), handling pathlist generation for VALUES clauses in PostgreSQL's query planner.

## Definition
```c
static void set_values_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
```

## Detailed Description
This function is responsible for creating access paths for VALUES RTEs in PostgreSQL's query optimizer. It handles the case where a VALUES clause is used as a data source in a query (e.g., SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)). The function is simpler compared to other pathlist functions as VALUES clauses have straightforward access patterns and do not support complex optimizations like join clause pushdown.

The function only needs to account for required parameterization due to LATERAL references in the values expressions, which can occur when VALUES clauses reference columns from outer queries in lateral joins.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global information about the query being planned
- `rel`: RelOptInfo structure representing the relation (VALUES clause) for which paths are being generated
- `rte`: RangeTblEntry representing the VALUES clause in the query's range table

## Dependencies
- Functions called/Symbols referenced:
  - add_path
  - create_valuesscan_path
- Called from (representative examples):
  - set_rel_pathlist

## Notes and Other Information
- VALUES scans do not support pushing join clauses into their quals, making them simpler than table or function scans
- Required parameterization can still occur due to LATERAL references in the values expressions
- The function generates a single, straightforward access path using create_valuesscan_path
- Located in src/backend/optimizer/path/allpaths.c:2816-2835