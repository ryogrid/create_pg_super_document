# markRTEForSelectPriv

## Location
src/backend/parser/parse_relation.c: 1066 - 1149

## Overview
Marks a specified column of a range table entry as requiring SELECT privilege for access control enforcement.

## Definition


## Detailed Description
The `markRTEForSelectPriv` function is responsible for tracking which columns require SELECT privileges during query parsing. It handles different types of range table entries (RTEs) differently based on their kind. For relation RTEs, it directly marks the column in the permission info structure. For join RTEs, it recursively marks the underlying base relations that contribute to the join.

When dealing with relation RTEs, the function updates the `RTEPermissionInfo` structure by setting the `ACL_SELECT` flag and adding the column to the `selectedCols` bitmap. For join RTEs with whole-row references, it recursively calls itself on the left and right inputs of the join. For ordinary column references in joins, no action is needed since the underlying columns will be marked through the join's qualification clause.

## Parameters / Member Variables
- `pstate`: The parse state containing range table and permission information
- `rtindex`: Index of the range table entry to mark
- `col`: Column number to mark, or InvalidAttrNumber for whole-row reference

## Dependencies
- Functions called/Symbols referenced:
  - rt_fetch
  - [getRTEPermissionInfo](../g/getRTEPermissionInfo.md)
  - [bms_add_member](../b/bms_add_member.md)
  - list_nth_node
  - nodeTag
  - RTE_RELATION
  - RTE_JOIN
  - [RTEPermissionInfo](../R/RTEPermissionInfo.md)
  - ACL_SELECT
  - FirstLowInvalidHeapAttributeNumber
  - InvalidAttrNumber
  - JoinExpr
  - RangeTblRef
- Called from (representative examples):
  - [markVarForSelectPriv](markVarForSelectPriv.md)
  - [markRTEForSelectPriv](markRTEForSelectPriv.md) (recursive calls)

## Notes and Other Information
- The function is static (internal to parse_relation.c)
- Handles whole-row references by using InvalidAttrNumber as the column identifier
- For join RTEs, recursively processes left and right join inputs for whole-row references
- Column numbers are offset by FirstLowInvalidHeapAttributeNumber to fit in bitmapsets
- Does not require privilege marking for RTE types other than relations and joins
- Critical for PostgreSQL's row-level security and privilege checking system
- The function maintains the principle that all accessed columns must be explicitly tracked for security auditing