# expandNSItemAttrs

## Location
src/backend/parser/parse_relation.c: 3187 - 3252

## Overview
This function is the workhorse for "*" expansion in PostgreSQL's parser, producing a list of target entries for all attributes of a given namespace item.

## Definition


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
  - expandNSItemVars
  - makeTargetEntry
  - markVarForSelectPriv
  - forboth (macro)
  - RTE_RELATION
  - ACL_SELECT
- Called from (representative examples):
  - transformValuesClause
  - ExpandAllTables
  - ExpandSingleTable

## Notes and Other Information
- The function automatically handles permission checking for table access, marking the relation as requiring ACL_SELECT permission
- For relations (not joins), it ensures SELECT permission is granted even if the table has zero columns
- The function maintains assertion checks to ensure the names and variables lists have matching lengths
- Result numbers are automatically assigned and incremented via 
- This function is essential for implementing SQL's "*" wildcard functionality in SELECT statements