# translate_col_privs_multilevel

## Location
src/backend/optimizer/util/inherit.c: 760 - 798

## Overview
Recursively translates column number privileges from a parent relation to a descendant relation through multiple levels of inheritance hierarchy.

## Definition


## Detailed Description
This function handles column privilege translation across multiple levels of table inheritance. It recursively traverses the inheritance hierarchy from a descendant relation up to a specified parent relation, translating column numbers at each level. The function is designed to handle complex inheritance chains where there may be multiple intermediate parent relations between the target relation and the top-level parent.

The function works by first checking if the immediate parent of the target relation is the desired parent relation. If not, it recursively calls itself to translate privileges from the top parent down to the immediate parent, then performs the final translation from immediate parent to the target relation.

Note that like its underlying translate_col_privs function, this will expand whole-row references into all inherited columns, which is acceptable for current PostgreSQL usages but should be considered when extending functionality.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and append relation information
- : Target RelOptInfo representing the descendant relation to translate privileges to
- : RelOptInfo representing the ancestor relation to translate privileges from  
- : Bitmapset of column numbers in the parent relation's attribute numbering

## Dependencies
- Functions called/Symbols referenced:
  - translate_col_privs_multilevel (recursive call)
  - translate_col_privs
  - AppendRelInfo (data structure)
- Called from (representative examples):
  - get_rel_all_updated_cols
  - translate_col_privs_multilevel (recursive)

## Notes and Other Information
- Fast path optimization returns NULL immediately if parent_cols is NULL
- Requires root->append_rel_array to be properly initialized
- Uses Assert statements to verify append relation information exists
- Handles error case where relation is not properly configured as a child relation
- Part of PostgreSQL's inheritance and partitioning privilege management system
- Located in src/backend/optimizer/util/inherit.c at lines 760-798