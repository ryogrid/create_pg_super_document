# set_dummy_rel_pathlist

## Location
src/backend/optimizer/path/allpaths.c: 2166 - 2213

## Overview
Creates a dummy path for a relation that has been excluded by constraints, representing it as an empty AppendPath with zero rows.

## Definition
```c
static void set_dummy_rel_pathlist(RelOptInfo *rel)
```

## Detailed Description
This function is used in the PostgreSQL query optimizer to handle relations that have been determined to produce no rows due to constraint exclusion or other optimization techniques. Instead of creating a special "dummy" path type, PostgreSQL represents dummy relations using an AppendPath with no subpaths. The function sets the relation's size estimates to zero, clears any existing paths, creates an empty AppendPath, and updates the cheapest path information. This approach allows the rest of the optimizer to handle dummy relations uniformly without special case handling.

## Parameters / Member Variables
- `rel`: Pointer to RelOptInfo structure representing the relation to be marked as dummy. The function modifies this structure's pathlist, size estimates, and cheapest path fields.

## Dependencies
- Functions called/Symbols referenced:
  - add_path (path management function)
  - create_append_path (creates AppendPath with empty subpath list)
  - set_cheapest (updates cheapest path fields)
  - NIL (empty list constant)
- Called from (representative examples):
  - pushdown_safe_type
  - set_rel_size
  - set_append_rel_size
  - set_subquery_pathlist

## Notes and Other Information
- This is a static function accessible only within allpaths.c
- Works in conjunction with IS_DUMMY_APPEND and IS_DUMMY_REL macros for identification
- Similar to mark_dummy_rel but used during initial path generation rather than converting existing paths
- Sets rel->rows = 0 and rel->reltarget->width = 0 to indicate empty result set
- The function immediately calls set_cheapest() for safety, though this may be redundant with later set_rel_pathlist calls
- Located in src/backend/optimizer/path/allpaths.c at lines 2166-2213
- Critical for constraint exclusion optimization where entire partitions can be eliminated