# addNSItemToQuery

## Location
[src/backend/parser/parse_relation.c:2619-2658](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L2619-L2658)

## Overview
Adds a given namespace item (nsitem) and its corresponding Range Table Entry (RTE) as a top-level entry in the parser state's join list and/or namespace list, with configurable visibility settings.

## Definition

```c
void
addNSItemToQuery(ParseState *pstate, ParseNamespaceItem *nsitem,
				 bool addToJoinList,
				 bool addToRelNameSpace, bool addToVarNameSpace)
```
## Detailed Description
This function is a key component of PostgreSQL's parser infrastructure that manages namespace visibility and join relationships during query parsing. It takes a ParseNamespaceItem (which represents a relation in the query's namespace) and optionally adds it to two critical parser state structures:

1. **Join List**: When  is true, creates a RangeTblRef node and appends it to the parser state's join list, making the relation participate in the query's FROM clause processing.

2. **Namespace List**: When either  or  is true, adds the nsitem to the parser state's namespace list with appropriate visibility flags set.

The function ensures that the namespace item is marked as unconditionally visible (not LATERAL-only), meaning it can be referenced from any part of the query without LATERAL restrictions.

## Parameters / Member Variables
- : Parser state containing the current parsing context, including join lists and namespace lists
- : The ParseNamespaceItem to be added, representing a relation and its associated metadata
- : Boolean flag indicating whether to add the item to the join list for FROM clause processing
- : Boolean flag indicating whether the relation name should be visible for relation references
- : Boolean flag indicating whether the relation's columns should be visible for variable references

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates RangeTblRef node)
  - lappend (appends to lists)
- Data structures used:
  - [ParseNamespaceItem](../P/ParseNamespaceItem.md)
  - RangeTblRef
  - [ParseState](../P/ParseState.md)
- Called from (representative examples):
  - [transformInsertStmt](../t/transformInsertStmt.md) (in INSERT statement processing)
  - [setTargetTable](../s/setTargetTable.md) (when setting up target tables)
  - [transformMergeStmt](../t/transformMergeStmt.md) (in MERGE statement processing)
  - [DefineRelation](../D/DefineRelation.md) (during table creation)
  - [CreatePolicy](../C/CreatePolicy.md) (when creating row-level security policies)

## Notes and Other Information
- The function assumes that the caller has already checked for namespace conflicts before calling
- The nsitem is always marked with  and , indicating unrestricted visibility
- This function is widely used throughout the parser for various SQL statement types (INSERT, MERGE, CREATE TABLE, etc.)
- The separation of  and  allows fine-grained control over what aspects of a relation are visible in different parsing contexts
- Part of PostgreSQL's namespace management system that ensures proper scoping and visibility of relations and their attributes during query parsing