# generate_nonunion_paths

## Location
[src/backend/optimizer/prep/prepunion.c:1018-1207](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepunion.c#L1018-L1207)

## Overview
Generates execution paths for INTERSECT, INTERSECT ALL, EXCEPT, and EXCEPT ALL operations by recursively processing left and right operands and choosing optimal hash or sort-based strategies.

## Definition


## Detailed Description
This function handles path generation for set operations that require comparing two input sets (INTERSECT and EXCEPT operations). It follows a systematic approach:

1. **Child Processing**: Forces tuple_fraction to 0.0 to ensure children fetch all tuples, then recursively processes left and right operands using 
2. **Input Ordering**: Determines optimal input order - for EXCEPT, left input must be first; for INTERSECT, smaller input (fewer groups) is placed first to minimize hash table size
3. **Path Construction**: Creates an Append path combining both child paths, with proper target list generation including a flag column for set operation processing
4. **Strategy Selection**: Uses  to decide between hash-based or sort-based execution strategy based on estimated costs and data characteristics
5. **Final Path Creation**: Adds appropriate sort path if needed, then creates the final SetOp path node with the chosen strategy and proper SetOpCmd

The function handles both ALL and non-ALL variants of INTERSECT and EXCEPT operations, with different row estimation strategies for each.

## Parameters / Member Variables
- : SetOperationStmt containing the operation type (INTERSECT/EXCEPT), ALL flag, column types and collations
- : PlannerInfo providing global planning context and configuration settings
- : List of reference names for constructing the target list
- : Output parameter returning the generated target list for the operation

## Dependencies
- Functions called/Symbols referenced:
  - [recurse_set_operations](../r/recurse_set_operations.md)
  - [build_setop_child_paths](../b/build_setop_child_paths.md)
  - [generate_append_tlist](generate_append_tlist.md)
  - fetch_upper_rel
  - create_pathtarget
  - [create_append_path](../c/create_append_path.md)
  - [generate_setop_grouplist](generate_setop_grouplist.md)
  - [choose_hashed_setop](../c/choose_hashed_setop.md)
  - [create_sort_path](../c/create_sort_path.md)
  - [make_pathkeys_for_sortclauses](../m/make_pathkeys_for_sortclauses.md)
  - [create_setop_path](../c/create_setop_path.md)
- Called from (representative examples):
  - [recurse_set_operations](../r/recurse_set_operations.md)

## Notes and Other Information
- The function temporarily sets root->tuple_fraction to 0.0 to ensure complete data retrieval from children
- For EXCEPT operations, the left input order is mandatory; for INTERSECT, input order is optimized based on size
- Row estimates are conservative worst-case calculations: non-ALL cases estimate one output row per group, ALL cases use relevant relation size
- The generated target list includes a special flag column that must appear as a variable (not constant) to avoid confusion in later planning phases
- Hash vs. sort strategy selection considers factors like data size, available memory, and cost estimates