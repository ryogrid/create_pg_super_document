# markVarForSelectPriv

## Location
src/backend/parser/parse_relation.c: 1150 - 1176

## Overview
Marks the range table entry referenced by a Var node as requiring SELECT privilege for the Var's column.

## Definition


## Detailed Description
The `markVarForSelectPriv` function serves as a wrapper around `markRTEForSelectPriv` that handles Var nodes specifically. It extracts the necessary information from a Var node (relation index and column attribute number) and delegates the actual privilege marking to `markRTEForSelectPriv`. The function also handles multi-level query nesting by traversing parent parse states when the Var references an outer query level.

This function is a key component in PostgreSQL's access control system, ensuring that all column references in a query are properly tracked for privilege checking. It works with both regular column references and whole-row references, making it versatile for different types of column access patterns in SQL queries.

## Parameters / Member Variables
- `pstate`: The parse state containing range table and permission information
- `var`: The Var node containing the relation and column information to mark

## Dependencies
- Functions called/Symbols referenced:
  - [markRTEForSelectPriv](markRTEForSelectPriv.md)
- Called from (representative examples):
  - [transformJoinUsingClause](../t/transformJoinUsingClause.md)
  - transformWholeRowRef
  - [scanNSItemForColumn](../s/scanNSItemForColumn.md)
  - [expandNSItemAttrs](../e/expandNSItemAttrs.md)
  - [ExpandSingleTable](../E/ExpandSingleTable.md)

## Notes and Other Information
- The function includes an Assert to ensure the input is actually a Var node
- Handles uplevel Vars by traversing parent parse states using the `varlevelsup` field
- Acts as a convenient interface for marking privileges on Var nodes without requiring callers to extract relation and column information manually
- Essential for PostgreSQL's row-level security and column-level access control
- The function preserves the original parse state context while navigating to the appropriate level for privilege marking
- Works seamlessly with both regular column references and whole-row references (when varattno is 0)