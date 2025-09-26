# transformWholeRowRef

## Location
src/backend/parser/parse_expr.c: 2620 - 2691

## Overview
Constructs a whole-row reference to represent the notation "relation.*" by creating either a whole-row Var or expanding to a RowExpr for JOIN USING aliases.

## Definition

```c
static Node *
transformWholeRowRef(ParseState *pstate, ParseNamespaceItem *nsitem,
					 int sublevels_up, int location)
```
## Detailed Description
The  function constructs references for "relation.*" notation during parsing. It handles two main cases: normal relations where it creates a whole-row Var, and JOIN USING aliases where it expands the reference into a RowExpr containing only the subset of columns available through the alias.

For normal relations, the function creates a whole-row Var using , marks it for nullability if needed, and ensures SELECT privileges are recorded. For JOIN USING aliases (where p_names differs from the RTE's eref), it expands the RTE and creates a RowExpr with only the columns listed in the alias's colnames, maintaining proper type information.

The function also handles the special case of scalar functions, where it creates a plain reference to the function value rather than a composite containing a single column, maintaining historical behavior for consistency.

## Parameters / Member Variables
- : ParseState context for the current parsing operation
- : ParseNamespaceItem representing the relation being referenced
- : Number of query levels up from the current level
- : Source location for error reporting

## Dependencies
- Functions called/Symbols referenced:
  - makeWholeRowVar
  - markNullableIfNeeded
  - markVarForSelectPriv
  - expandRTE
  - makeNode
  - list_truncate
  - list_length
  - copyObject
- Called from (representative examples):
  - Column reference resolution functions (multiple locations in parse_expr.c)

## Notes and Other Information
- Handles both normal relations and JOIN USING aliases differently
- For scalar functions, creates plain reference instead of composite
- Maintains historical behavior for "rel" vs "rel.*" equivalence
- Sets proper location information and nullability markers
- Records SELECT privilege requirements for accessed columns
- Uses RECORDOID and COERCE_IMPLICIT_CAST for RowExpr type information
- Located in src/backend/parser/parse_expr.c:2620-2691