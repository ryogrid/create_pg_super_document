# expandNSItemVars

## Location
[src/backend/parser/parse_relation.c:3123-3186](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L3123-L3186)

## Overview
Produces a list of Var nodes and optionally column names for the non-dropped, expandable columns of a ParseNamespaceItem, with proper nullability marking and location information.

## Definition


## Detailed Description
This function is a sophisticated column expansion utility that works with ParseNamespaceItem structures to generate properly configured Var nodes. Unlike lower-level expansion functions that work with tuple descriptors or RTEs directly, this function operates on namespace items which contain pre-processed column metadata including nullability information and syntactic aliasing details.

The function performs several critical operations:

1. **Selective Expansion**: Only processes columns that are not marked with p_dontexpand and are not dropped (have non-empty names)
2. **Advanced Var Creation**: Creates Var nodes with complete metadata including varnosyn and varattnosyn for syntactic representation
3. **Nullability Analysis**: Calls markNullableIfNeeded to update varnullingrels based on the current parse state and join context
4. **Column Name Extraction**: Optionally returns the actual column name strings (not copies) from the namespace item

This function is particularly important for handling complex query scenarios involving outer joins, subqueries, and other constructs where column nullability and syntactic representation matter.

## Parameters / Member Variables
- : Parser state containing join information and nullability context needed for markNullableIfNeeded
- : ParseNamespaceItem containing pre-processed column metadata and name information
- : Nesting level for created Var nodes, indicating subquery depth
- : Source location information to attach to created Var nodes for error reporting
- : Optional output parameter for column name list (pass NULL if not needed); returns pointers to original strings, not copies

## Dependencies
- Functions called/Symbols referenced:
  - makeVar (creates basic Var nodes)
  - [markNullableIfNeeded](../m/markNullableIfNeeded.md) (updates nullability information based on join context)
  - lappend (list append operations)
  - strVal (extracts string values)
- Data structures used:
  - [ParseNamespaceItem](../P/ParseNamespaceItem.md), ParseNamespaceColumn (namespace metadata structures)
  - String (PostgreSQL string node type)
  - Var (variable reference nodes)
- Called from (representative examples):
  - [transformInsertStmt](../t/transformInsertStmt.md) (processing INSERT statements)
  - [ExpandSingleTable](../E/ExpandSingleTable.md) (target list expansion)
  - [coerce_record_to_complex](../c/coerce_record_to_complex.md) (type coercion operations)
  - [expandNSItemAttrs](expandNSItemAttrs.md) (attribute expansion)

## Notes and Other Information
- Returns a new list of Var nodes; the list itself is newly allocated but column names (if requested) are pointers to original strings
- The function skips columns marked with p_dontexpand, which may be set for various parser-internal reasons
- Dropped columns are identified by empty string names and p_varno == 0 in the ParseNamespaceColumn
- Sets both varnosyn/varattnosyn (syntactic representation) and varno/varattno (semantic representation) in Var nodes
- The markNullableIfNeeded call is crucial for correctly handling outer join semantics and column nullability
- More sophisticated than expandRTE because it works with pre-processed namespace information rather than raw relation metadata
- Essential for scenarios where the relationship between syntactic and semantic column references differs (e.g., after view expansion or join processing)
- Part of PostgreSQL's advanced namespace management system that handles complex query transformations