# generate_recursion_path

## Location
[src/backend/optimizer/prep/prepunion.c:384-503](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepunion.c#L384-L503)

## Overview
Generates optimized execution paths for recursive UNION operations, specifically handling Common Table Expressions (CTEs) with recursive semantics in PostgreSQL.

## Definition

```c
union(lrel->relids, rrel->relids));
```
## Detailed Description
 is a specialized function that creates execution paths for recursive UNION operations, which are used to implement recursive Common Table Expressions (CTEs) in PostgreSQL. Unlike regular UNION operations that can be processed by combining inputs into an Append node, recursive UNIONs require special handling with a RecursiveUnion execution node.

The function processes recursive UNIONs by:
- Separately processing the left (anchor/non-recursive) and right (recursive) branches of the UNION
- Setting up the non-recursive path reference for the recursive branch to access
- Generating appropriate target lists for both branches
- Creating a RecursiveUnion path node that can iteratively execute the recursive part
- Handling grouping operations for duplicate elimination when UNION (not UNION ALL) is specified
- Validating that all column datatypes are hashable when duplicate elimination is needed

The key insight is that recursive queries work by first executing the non-recursive term (anchor), then iteratively executing the recursive term using the results from previous iterations until no new rows are produced.

## Parameters / Member Variables
- : SetOperationStmt representing the recursive UNION operation
- : PlannerInfo containing the overall query planning context, including the worktable parameter ID
- : Target list providing column names for the result
- : Output parameter receiving the target list for the recursive union result

## Dependencies
- Functions called/Symbols referenced:
  - [recurse_set_operations](../r/recurse_set_operations.md)
  - [build_setop_child_paths](../b/build_setop_child_paths.md)  
  - [generate_append_tlist](generate_append_tlist.md)
  - [fetch_upper_rel](../f/fetch_upper_rel.md)
  - create_pathtarget
  - [generate_setop_grouplist](generate_setop_grouplist.md)
  - [grouping_is_hashable](grouping_is_hashable.md)
  - [create_recursiveunion_path](../c/create_recursiveunion_path.md)
  - [add_path](../a/add_path.md)
  - [postprocess_setop_rel](../p/postprocess_setop_rel.md)
  - list_make2
  - [bms_union](../b/bms_union.md)
- Called from (representative examples):
  - [plan_set_operations](../p/plan_set_operations.md) (src/backend/optimizer/prep/prepunion.c:152)

## Notes and Other Information
- This is a static function, internal to the prepunion.c module
- The function only handles UNION operations; other set operations cannot be recursive
- Requires that a worktable parameter ID has been assigned (root->wt_param_id >= 0)
- For UNION ALL operations, no grouping is needed so the process is simpler
- For regular UNION operations, all column datatypes must be hashable for duplicate elimination
- The function estimates the number of distinct groups conservatively as the total input size (worst case)
- The non_recursive_path is temporarily stored in the PlannerInfo so the recursive branch can reference it
- Both left and right paths are processed through build_setop_child_paths if they represent subqueries
- The RecursiveUnion node will handle the iterative execution of the recursive part at runtime