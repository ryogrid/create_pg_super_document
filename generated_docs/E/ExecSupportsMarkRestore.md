# ExecSupportsMarkRestore

## Location
[src/backend/executor/execAmi.c:417-509](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execAmi.c#L417-L509)

## Overview
ExecSupportsMarkRestore determines whether a given Path supports mark/restore operations during query planning, enabling the planner to make informed decisions about join algorithms and node placement.

## Definition

```c
bool
ExecSupportsMarkRestore(Path *pathnode)
```
## Detailed Description
ExecSupportsMarkRestore is a planning-time function that analyzes a Path node to determine whether the resulting plan node will support mark/restore operations. This information is crucial for the query planner, particularly when considering MergeJoin operations, which require the ability to mark and restore positions in the inner relation.

The function examines the pathtype (which corresponds to the Plan node type that the Path would produce) rather than the nodeTag, maintaining consistency with runtime execution functions. It handles various path types with different mark/restore capabilities:

**Fully Supported Types:**
- **Material and Sort**: Always support mark/restore as they maintain complete data sets
- **IndexScan and IndexOnlyScan**: Support depends on the underlying index access method's capabilities (checked via amcanmarkpos)

**Conditionally Supported Types:**
- **CustomScan**: Support depends on the CUSTOMPATH_SUPPORT_MARK_RESTORE flag
- **Result**: Support depends on whether it has a child and if that child supports mark/restore
- **Append**: Only supports mark/restore if it has exactly one subpath (in which case the Append node will be optimized away)
- **MergeAppend**: Similar to Append - only supports mark/restore with a single subpath

**Recursive Analysis:**
For complex path types like ProjectionPath, the function recursively checks the underlying subpath's capabilities. This ensures accurate assessment even through multiple layers of path transformations.

## Parameters / Member Variables
- `*pathnode`: Pointer to the Path node being analyzed. The function examines the pathtype field to determine the corresponding plan node type and assess mark/restore support.
## Dependencies
- Functions called/Symbols referenced:
  - castNode (safe type casting)
  - IsA (type checking)
  - [ExecSupportsMarkRestore](ExecSupportsMarkRestore.md) (recursive calls)
  - linitial (list access)
  - [list_length](../l/list_length.md) (list operations)
- Called from (representative examples):
  - [final_cost_mergejoin](../f/final_cost_mergejoin.md) (merge join costing)
  - Recursive self-calls for complex path analysis

## Notes and Other Information
- This function operates during query planning, not execution, so it works with Path nodes rather than PlanState nodes
- The function is used by the cost estimation routines to determine whether MergeJoin is feasible for a given path
- For index scans, support depends on the specific index access method - not all index types can mark positions
- Single-subpath Append and MergeAppend nodes will be optimized away during plan creation, so their mark/restore support is determined by their single child
- The function returns false for any unrecognized path types, providing conservative behavior
- Custom scan providers must explicitly set the CUSTOMPATH_SUPPORT_MARK_RESTORE flag to indicate support
- [Result](../R/Result.md) nodes with multiple path types (ProjectionPath, MinMaxAggPath, GroupResultPath) have different support characteristics

## Simplified Source

```c
bool ExecSupportsMarkRestore(Path *pathnode) {
    // Check pathtype (Plan node type) rather than nodeTag for consistency
    switch (pathnode->pathtype) {
        case T_IndexScan:
        case T_IndexOnlyScan:
            // Index support depends on access method capabilities
            return castNode(IndexPath, pathnode)->indexinfo->amcanmarkpos;

        case T_Material:
        case T_Sort:
            return true; // Always support mark/restore

        case T_CustomScan:
            // Support depends on custom scan provider flag
            return (castNode(CustomPath, pathnode)->flags & CUSTOMPATH_SUPPORT_MARK_RESTORE) != 0;

        case T_Result:
            // Support depends on child plan's capabilities
            if (IsA(pathnode, ProjectionPath))
                return ExecSupportsMarkRestore(((ProjectionPath *) pathnode)->subpath);
            else if (IsA(pathnode, MinMaxAggPath) || IsA(pathnode, GroupResultPath))
                return false; // Childless Result nodes
            else
                return false; // Simple RTE_RESULT base relation

        case T_Append:
            {
                AppendPath *appendPath = castNode(AppendPath, pathnode);
                // Single-child Append will be optimized away, check child
                if (list_length(appendPath->subpaths) == 1)
                    return ExecSupportsMarkRestore((Path *) linitial(appendPath->subpaths));
                return false; // Multi-child Append doesn't support it
            }

        case T_MergeAppend:
            {
                MergeAppendPath *mapath = castNode(MergeAppendPath, pathnode);
                // Single-child MergeAppend will be optimized away, check child
                if (list_length(mapath->subpaths) == 1)
                    return ExecSupportsMarkRestore((Path *) linitial(mapath->subpaths));
                return false; // Multi-child MergeAppend doesn't support it
            }

        default:
            return false; // Conservative default for unrecognized types
    }
}
```