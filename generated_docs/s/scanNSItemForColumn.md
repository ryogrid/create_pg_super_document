# scanNSItemForColumn

## Location
[src/backend/parser/parse_relation.c:680-799](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L680-L799)

## Overview
Searches for a column name within a single namespace item and returns an appropriate Var node if found, with comprehensive access control validation.

## Definition

```c
Node *
scanNSItemForColumn(ParseState *pstate, ParseNamespaceItem *nsitem,
					int sublevels_up, const char *colname, int location)
```
## Detailed Description
This function performs column name resolution within a specific namespace item (typically representing a table, view, or subquery in the FROM clause). It searches for the specified column name within the item's column names or aliases, applying various access control checks and expression context restrictions. The function handles both regular user columns and system columns, ensuring proper privilege checking and context-specific validation (such as restrictions in CHECK constraints, generated columns, and MERGE WHEN conditions). Upon successful match, it constructs and returns a Var node representing the column reference.

## Parameters / Member Variables
- : ParseState pointer representing the current parser state context
- : ParseNamespaceItem pointer representing the specific namespace item to search
- : Integer indicating query nesting level for the variable reference
- : String containing the column name to search for
- : Integer representing the parse location for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - [ParseNamespaceItem](../P/ParseNamespaceItem.md)
  - [scanRTEForColumn](scanRTEForColumn.md)
  - InvalidAttrNumber
  - EXPR_KIND_CHECK_CONSTRAINT
  - TableOidAttributeNumber
  - EXPR_KIND_GENERATED_COLUMN
  - EXPR_KIND_MERGE_WHEN
  - [ParseNamespaceColumn](../P/ParseNamespaceColumn.md)
  - [makeVar](../m/makeVar.md)
  - [SystemAttributeDefinition](../S/SystemAttributeDefinition.md)
  - [markNullableIfNeeded](../m/markNullableIfNeeded.md)
  - [markVarForSelectPriv](../m/markVarForSelectPriv.md)
- Called from (representative examples):
  - [CRERR_TOO_MANY](../C/CRERR_TOO_MANY.md) (parse expression error handling)
  - [ParseComplexProjection](../P/ParseComplexProjection.md)
  - [colNameToVar](../c/colNameToVar.md)

## Notes and Other Information
- Returns NULL if no column match is found
- Implements strict access control for system columns in various expression contexts
- Handles both user-defined columns and system columns with different type resolution strategies
- Marks variables for appropriate privilege checking via markVarForSelectPriv
- Applies nullability marking based on outer join context
- Essential component of PostgreSQL's column name resolution mechanism
- Located in src/backend/parser/parse_relation.c:680-799