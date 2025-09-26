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
- : Path structure to analyze for enforced clauses

## Dependencies
- Functions called/Symbols referenced:
  - IsA (for NestPath, MergePath, HashPath, AppendPath, MergeAppendPath)
  - bms_add_members
  - bms_add_member
  - bms_copy
  - bms_int_members
  - list_head
  - get_param_path_clause_serials (recursive calls)
- Called from (representative examples):
  - create_nestloop_path
  - get_param_path_clause_serials (recursive self-calls)

## Notes and Other Information
- Returns NULL for unparameterized paths (when path->param_info is NULL)
- Uses union logic for join paths to combine clauses from all sources
- Uses intersection logic for append paths to find commonly enforced clauses
- Recursive function that traverses the path tree structure
- Serial numbers provide a way to uniquely identify and track clause enforcement
- For join paths, may include some non-pushed-down clauses in the result for efficiency
- The function is located in src/backend/optimizer/util/relnode.c:1922-2016