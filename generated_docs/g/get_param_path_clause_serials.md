# get_param_path_clause_serials

## Location
[src/backend/optimizer/util/relnode.c:1922-2016](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/relnode.c#L1922-L2016)

## Overview
Extract the set of pushed-down clause serial numbers that are enforced within a parameterized path, handling different path types with appropriate aggregation logic.

## Definition

```c
Bitmapset *
get_param_path_clause_serials(Path *path)
```
## Detailed Description
This recursive function analyzes a parameterized path to determine which clauses (identified by their rinfo_serial numbers) are enforced within the path. The function handles different path types with specific logic:

For join paths (NestPath, MergePath, HashPath), it combines clauses from both input paths plus any join restriction clauses applied at the join level. For append paths (AppendPath, MergeAppendPath), it computes the intersection of clauses enforced by all subpaths, representing clauses that are guaranteed to be checked regardless of which subpath is taken. For base relation paths, it returns the precomputed serial numbers from the ParamPathInfo.

The function is essential for determining clause redundancy and ensuring proper constraint enforcement in complex parameterized query plans.

## Parameters / Member Variables
- `*path`: Path structure to analyze for enforced clauses
## Dependencies
- Functions called/Symbols referenced:
  - IsA (for NestPath, MergePath, HashPath, AppendPath, MergeAppendPath)
  - [bms_add_members](../b/bms_add_members.md)
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_copy](../b/bms_copy.md)
  - [bms_int_members](../b/bms_int_members.md)
  - [list_head](../l/list_head.md)
  - [get_param_path_clause_serials](get_param_path_clause_serials.md) (recursive calls)
- Called from (representative examples):
  - [create_nestloop_path](../c/create_nestloop_path.md)
  - [get_param_path_clause_serials](get_param_path_clause_serials.md) (recursive self-calls)

## Notes and Other Information
- Returns NULL for unparameterized paths (when path->param_info is NULL)
- Uses union logic for join paths to combine clauses from all sources
- Uses intersection logic for append paths to find commonly enforced clauses
- Recursive function that traverses the path tree structure
- Serial numbers provide a way to uniquely identify and track clause enforcement
- For join paths, may include some non-pushed-down clauses in the result for efficiency
- The function is located in src/backend/optimizer/util/relnode.c:1922-2016

## Simplified Source

```c
Bitmapset *get_param_path_clause_serials(Path *path) {
    // Return NULL for unparameterized paths
    if (path->param_info == NULL)
        return NULL;

    // Handle join paths (NestPath, MergePath, HashPath)
    if (IsA(path, NestPath) || IsA(path, MergePath) || IsA(path, HashPath)) {
        JoinPath *jpath = (JoinPath *) path;
        Bitmapset *pserials = NULL;
        ListCell *lc;

        // Combine clauses from both input paths
        pserials = bms_add_members(pserials, get_param_path_clause_serials(jpath->outerjoinpath));
        pserials = bms_add_members(pserials, get_param_path_clause_serials(jpath->innerjoinpath));

        // Add join restriction clauses
        foreach(lc, jpath->joinrestrictinfo) {
            RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
            pserials = bms_add_member(pserials, rinfo->rinfo_serial);
        }
        return pserials;
    }
    // Handle append paths (AppendPath, MergeAppendPath)
    else if (IsA(path, AppendPath) || IsA(path, MergeAppendPath)) {
        List *subpaths = IsA(path, AppendPath) ?
                        ((AppendPath *) path)->subpaths :
                        ((MergeAppendPath *) path)->subpaths;
        Bitmapset *pserials = NULL;
        ListCell *lc;

        // Take intersection of clauses enforced in all subpaths
        foreach(lc, subpaths) {
            Path *subpath = (Path *) lfirst(lc);
            Bitmapset *subserials = get_param_path_clause_serials(subpath);

            if (lc == list_head(subpaths))
                pserials = bms_copy(subserials);
            else
                pserials = bms_int_members(pserials, subserials);
        }
        return pserials;
    }
    // Base relation path: use precomputed serials
    else {
        return path->param_info->ppi_serials;
    }
}
```