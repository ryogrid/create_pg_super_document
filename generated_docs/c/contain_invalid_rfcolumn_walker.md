# contain_invalid_rfcolumn_walker

## Location
src/backend/commands/publicationcmds.c: 219 - 257

## Overview
A tree walker function that checks whether any column referenced in a row filter expression is not part of the table's REPLICA IDENTITY.

## Definition


## Detailed Description
This function implements a recursive tree walker that traverses expression nodes to identify Var nodes (column references) and validates whether each referenced column is part of the table's REPLICA IDENTITY. It's specifically designed to validate row filter expressions used in logical replication publications.

The function handles a special case for partitioned tables when publish_via_partition_root is enabled. In this scenario, the row filter is defined on the parent table but needs to be validated against the child table's replica identity. The function performs column name resolution between parent and child tables since their column ordering may differ.

For each Var node encountered, it checks if the column's attribute number (adjusted for heap attribute numbering) is present in the replica identity bitmap. If any column is found that's not part of the replica identity, the function returns true, indicating the row filter contains invalid column references.

## Parameters / Member Variables
- : The expression tree node being examined (can be any Node type)
- : rf_context structure containing validation context including table IDs, replica identity bitmap, and pubviaroot flag

## Dependencies
- Functions called/Symbols referenced:
  - get_attname
  - get_attnum
  - bms_is_member
  - expression_tree_walker
  - FirstLowInvalidHeapAttributeNumber
  - rf_context
- Called from (representative examples):
  - contain_invalid_rfcolumn_walker (recursive)
  - pub_rf_contains_invalid_column

## Notes and Other Information
- Returns true if any referenced column is NOT in the replica identity, false otherwise
- Handles column mapping between parent and child tables when pubviaroot is enabled
- Uses FirstLowInvalidHeapAttributeNumber offset to adjust attribute numbers for bitmap indexing
- Recursively processes the entire expression tree using expression_tree_walker
- Specifically processes Var nodes while ignoring other node types during traversal
- Located in src/backend/commands/publicationcmds.c:219-257