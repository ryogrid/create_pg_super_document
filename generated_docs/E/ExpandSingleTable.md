# ExpandSingleTable

## Location
[src/backend/parser/parse_target.c:1372-1422](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_target.c#L1372-L1422)

## Overview
Transforms a qualified star expression (foo.*) into a list of column expressions or target list entries when foo refers to a simple table reference.

## Definition

```c
static List *
ExpandSingleTable(ParseState *pstate, ParseNamespaceItem *nsitem,
				  int sublevels_up, int location, bool make_target_entry)
```
## Detailed Description
ExpandSingleTable handles the expansion of star expressions (.*) when the prefix has been determined to reference a simple table (RTE - Range Table Entry). It generates appropriate Var nodes for each column in the referenced table and ensures proper access control by marking the referenced columns as requiring SELECT privileges.

The function operates in two modes based on the make_target_entry parameter:
1. When make_target_entry is true, it delegates to expandNSItemAttrs to create target list entries
2. When false, it creates a list of Var nodes using expandNSItemVars and manually handles permission checking

For permission management, the function ensures SELECT access is granted at both the table level (for tables with zero columns) and individual column level through markVarForSelectPriv calls.

## Parameters / Member Variables
- : Parse state containing context information for the current parsing operation
- : ParseNamespaceItem representing the table reference being expanded
- : Number of query nesting levels to traverse upward for variable resolution
- : Source location in the query text for error reporting purposes
- : Boolean flag determining whether to create TargetEntry structures (true) or simple Var nodes (false)

## Dependencies
- Functions called/Symbols referenced:
  - [expandNSItemAttrs](../e/expandNSItemAttrs.md)
  - [expandNSItemVars](../e/expandNSItemVars.md)  
  - [markVarForSelectPriv](../m/markVarForSelectPriv.md)
  - [ParseNamespaceItem](../P/ParseNamespaceItem.md)
  - [RTEPermissionInfo](../R/RTEPermissionInfo.md)
  - RTE_RELATION
  - ACL_SELECT
- Called from (representative examples):
  - [ExpandRowReference](ExpandRowReference.md)

## Notes and Other Information
- This is a static function within parse_target.c, indicating it's an internal helper for target list processing
- The function carefully handles permission requirements, ensuring both table-level and column-level SELECT privileges are properly marked
- Special consideration is given to tables with zero columns, where table-level permission marking is essential
- The function assumes the namespace item refers to a simple table reference rather than a complex expression or join