# expandNSItemAttrs

## Location
[src/backend/parser/parse_relation.c:3187-3252](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L3187-L3252)

## Overview
This function is the workhorse for "*" expansion in PostgreSQL's parser, producing a list of target entries for all attributes of a given namespace item.

## Definition

```c
List *
expandNSItemAttrs(ParseState *pstate, ParseNamespaceItem *nsitem,
				  int sublevels_up, bool require_col_privs, int location)
```
## Detailed Description
The  function expands a "*" reference into a list of  nodes representing all visible columns of a table or other relation. It serves as the core implementation for SELECT * operations and similar wildcard expansions in SQL queries. The function handles permission checking, assigns result numbers to target list entries, and marks columns as requiring SELECT access when requested.

The function works by first calling  to get the list of variables and column names, then creates  structures for each column with appropriate result numbers assigned from .

## Parameters / Member Variables
- : Parse state containing context information including the next available result number
- : ParseNamespaceItem representing the table/relation whose attributes should be expanded
- : Number of query levels up to look for the relation (for nested queries)
- : Boolean flag indicating whether to mark columns as requiring SELECT privileges
- : Source location in the query for error reporting purposes

## Dependencies
- Functions called/Symbols referenced:
  - [expandNSItemVars](expandNSItemVars.md)
  - [makeTargetEntry](../m/makeTargetEntry.md)
  - [markVarForSelectPriv](../m/markVarForSelectPriv.md)
  - forboth (macro)
  - RTE_RELATION
  - ACL_SELECT
- Called from (representative examples):
  - [transformValuesClause](../t/transformValuesClause.md)
  - [ExpandAllTables](../E/ExpandAllTables.md)
  - [ExpandSingleTable](../E/ExpandSingleTable.md)

## Notes and Other Information
- The function automatically handles permission checking for table access, marking the relation as requiring ACL_SELECT permission
- For relations (not joins), it ensures SELECT permission is granted even if the table has zero columns
- The function maintains assertion checks to ensure the names and variables lists have matching lengths
- [Result](../R/Result.md) numbers are automatically assigned and incremented via 
- This function is essential for implementing SQL's "*" wildcard functionality in SELECT statements