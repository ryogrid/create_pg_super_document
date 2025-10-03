# create_namedtuplestorescan_path

## Location
[src/backend/optimizer/util/pathnode.c:2150-2175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/pathnode.c#L2150-L2175)

## Overview
Creates a path node for scanning a named tuplestore, which represents an in-memory storage mechanism used for temporary data during query execution.

## Definition

```c
Path *
create_namedtuplestorescan_path(PlannerInfo *root, RelOptInfo *rel,
								Relids required_outer)
```
## Detailed Description
This function constructs a Path node specifically for named tuplestore scan operations. Named tuplestores are temporary storage structures that hold intermediate results during query processing, often used for operations like window functions, recursive CTEs, or other complex query constructs that require temporary data storage. The function initializes all necessary Path structure fields and sets the pathtype to T_NamedTuplestoreScan. Unlike some other scan types, named tuplestore scans always produce unordered results (pathkeys = NIL).

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing global information about the query being planned
- `*rel`: RelOptInfo structure representing the named tuplestore relation being scanned
- `required_outer`: Set of relation IDs that must be available as outer relations for this path
## Dependencies
- Functions called/Symbols referenced:
  - makeNode
  - [get_baserel_parampathinfo](../g/get_baserel_parampathinfo.md)
  - [cost_namedtuplestorescan](cost_namedtuplestorescan.md)
- Called from (representative examples):
  - [set_namedtuplestore_pathlist](../s/set_namedtuplestore_pathlist.md)

## Notes and Other Information
- Sets pathtype to T_NamedTuplestoreScan to identify this as a named tuplestore scan path
- Always sets pathkeys to NIL because results from named tuplestores are inherently unordered
- The path is marked as not parallel-aware but respects the relation's parallel safety settings
- No parallel workers are assigned (parallel_workers = 0)
- Cost calculation is handled by cost_namedtuplestorescan function
- Named tuplestores are primarily used for intermediate storage in complex query operations

## Simplified Source

This function follows the standard path creation pattern for tuplestores:

```c
Path *create_namedtuplestorescan_path(PlannerInfo *root, RelOptInfo *rel,
                                     Relids required_outer)
{
    Path *pathnode = makeNode(Path);

    // Set basic path properties
    pathnode->pathtype = T_NamedTuplestoreScan;
    pathnode->parent = rel;
    pathnode->pathtarget = rel->reltarget;
    pathnode->param_info = get_baserel_parampathinfo(root, rel, required_outer);

    // Set parallelism properties
    pathnode->parallel_aware = false;
    pathnode->parallel_safe = rel->consider_parallel;
    pathnode->parallel_workers = 0;

    // Tuplestores always produce unordered results
    pathnode->pathkeys = NIL;

    // Calculate costs
    cost_namedtuplestorescan(pathnode, root, rel, pathnode->param_info);

    return pathnode;
}
```

**Key simplifications made:**
- Grouped field assignments with descriptive comments
- Maintained the key difference from other paths (always unordered)
- Preserved all essential initialization steps
- Function follows the same pattern as create_ctescan_path but for tuplestore access