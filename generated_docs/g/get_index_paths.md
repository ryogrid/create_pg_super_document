# get_index_paths

## Location
[src/backend/optimizer/path/indxpath.c:710-803](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L710-L803)

## Overview
 constructs IndexPaths for a given index and set of index clauses, handling both plain index scans and bitmap index scans appropriately.

## Definition

```c
static void
get_index_paths(PlannerInfo *root, RelOptInfo *rel,
				IndexOptInfo *index, IndexClauseSet *clauses,
				List **bitindexpaths)
```
## Detailed Description
This function serves as a frontend to  with the primary purpose of properly handling ScalarArrayOpExpr qualifiers. It creates IndexPaths in two phases:

1. **Plain Index Paths**: First builds simple index paths using clauses, including ScalarArrayOpExpr clauses only if the index access method supports them natively
2. **Bitmap Index Paths**: Collects paths suitable for bitmap scans and handles ScalarArrayOpExpr clauses that cannot be processed natively

The function distinguishes between indexes that support tuple retrieval () for plain IndexScans and those that support bitmap retrieval () for bitmap scans. Plain IndexPaths are immediately submitted to , while bitmap-suitable paths are collected in the  list for later processing.

For ScalarArrayOpExpr clauses that cannot be handled natively by the index AM, the function makes a separate call to  specifically for bitmap scans, allowing executor-managed ScalarArrayOpExpr processing.

## Parameters / Member Variables
- : PlannerInfo containing planner state and configuration
- : RelOptInfo representing the relation being planned
- : IndexOptInfo describing the index being considered
- : IndexClauseSet containing the index clauses to be used
- : Output parameter - list of IndexPaths suitable for bitmap scans

## Dependencies
- Functions called/Symbols referenced:
  - [build_index_paths](../b/build_index_paths.md)
  - [add_path](../a/add_path.md)
  - [list_concat](../l/list_concat.md)
- Called from (representative examples):
  - [create_index_paths](../c/create_index_paths.md)
  - [get_join_index_paths](get_join_index_paths.md)

## Notes and Other Information
- The function handles the complexity of ScalarArrayOpExpr support, which varies by index access method
- Plain IndexPaths can represent either IndexScan or IndexOnlyScan operations
- Bitmap paths are only considered if they have selectivity (indexselectivity < 1.0) or no ordering requirements (pathkeys == NIL)
- The  flag tracks whether ScalarArrayOpExpr clauses need special bitmap scan handling

## Simplified Source

```c
static void
get_index_paths(PlannerInfo *root, RelOptInfo *rel,
               IndexOptInfo *index, IndexClauseSet *clauses,
               List **bitindexpaths)
{
    List *indexpaths;
    bool skip_nonnative_saop = false;
    ListCell *lc;

    // Build simple index paths using clauses
    // Allow ScalarArrayOpExpr clauses only if index AM supports them natively
    indexpaths = build_index_paths(root, rel,
                                  index, clauses,
                                  index->predOK,
                                  ST_ANYSCAN,
                                  &skip_nonnative_saop);

    // Process each generated index path
    foreach(lc, indexpaths)
    {
        IndexPath *ipath = (IndexPath *) lfirst(lc);

        // Submit paths that can form plain IndexScan plans to add_path
        // (covers both IndexScan and IndexOnlyScan)
        if (index->amhasgettuple)
            add_path(rel, (Path *) ipath);

        // Collect paths usable as bitmap scans
        // Must support bitmap scans and have selectivity or no ordering
        if (index->amhasgetbitmap &&
            (ipath->path.pathkeys == NIL ||
             ipath->indexselectivity < 1.0))
            *bitindexpaths = lappend(*bitindexpaths, ipath);
    }

    // Handle ScalarArrayOpExpr clauses that index can't handle natively
    // Generate bitmap scan paths with executor-managed ScalarArrayOpExpr
    if (skip_nonnative_saop)
    {
        indexpaths = build_index_paths(root, rel,
                                      index, clauses,
                                      false,
                                      ST_BITMAPSCAN,
                                      NULL);
        *bitindexpaths = list_concat(*bitindexpaths, indexpaths);
    }
}
```