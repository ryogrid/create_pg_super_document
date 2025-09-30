# ExpandIndirectionStar

## Location
[src/backend/parser/parse_target.c:1345-1371](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_target.c#L1345-L1371)

## Overview
Transforms "foo.*" expressions where "*" appears as the last item in an A_Indirection node into a list of expressions or target list entries.

## Definition

```c
static List *
ExpandIndirectionStar(ParseState *pstate, A_Indirection *ind,
					  bool make_target_entry, ParseExprKind exprKind)
```
## Detailed Description
This function handles the expansion of star expressions that involve indirection, such as "composite_column.*" or "function_returning_record().*". Unlike ExpandColumnRefStar which handles simple relation-qualified stars, this function deals with more complex expressions that require indirection processing.

The function operates in three main steps:

1. **Strip the Star**: Creates a copy of the A_Indirection node and removes the final "*" element using list_truncate(), leaving the base expression that should evaluate to a row type.

2. **Transform Base Expression**: Calls transformExpr() on the modified indirection to convert it into a proper expression tree that represents the row-type object.

3. **Expand Row Reference**: Uses ExpandRowReference() to expand the row-type expression into individual column expressions or target entries.

The function supports both target list contexts (where TargetEntry nodes are needed) and expression contexts (where bare expressions suffice) through the make_target_entry parameter. This flexibility allows it to handle both "SELECT foo.*" scenarios and "ROW(foo.*)" or "VALUES(foo.*)" constructs.

The use of copyObject() ensures that the original A_Indirection node is not modified, maintaining parser state integrity.

## Parameters / Member Variables
- : ParseState structure containing parsing context and state information
- : A_Indirection node representing the indirection expression ending with "*"
- : Boolean flag indicating whether to create TargetEntry nodes (true) or bare expressions (false)
- : ParseExprKind enumeration value specifying the expression context for proper transformation

## Dependencies
- Functions called/Symbols referenced:
  - copyObject
  - [list_truncate](../l/list_truncate.md)
  - [list_length](../l/list_length.md)
  - [transformExpr](../t/transformExpr.md)
  - [ExpandRowReference](ExpandRowReference.md)
- Called from (representative examples):
  - [transformTargetList](../t/transformTargetList.md) (src/backend/parser/parse_target.c:167)
  - [transformExpressionList](../t/transformExpressionList.md) (src/backend/parser/parse_target.c:256)

## Notes and Other Information
- The function is marked static, indicating it's an internal helper within parse_target.c
- The make_target_entry flag provides robustness by explicitly controlling output format rather than relying on exprKind inference
- This function complements ExpandColumnRefStar by handling more complex indirection patterns
- The copyObject() call prevents modification of the original AST node, which could affect other parsing operations
- The function assumes that the input A_Indirection node has at least one element (the "*") to truncate
- Error handling is delegated to the called functions (transformExpr and ExpandRowReference)
- This function enables PostgreSQL to support complex star expansions like "(SELECT composite_col FROM table).*"

## Simplified Source

```c
static List *
ExpandIndirectionStar(ParseState *pstate, A_Indirection *ind,
                      bool make_target_entry, ParseExprKind exprKind)
{
    Node *expr;

    // Strip off the '*' to create a reference to the rowtype object
    ind = copyObject(ind);
    ind->indirection = list_truncate(ind->indirection,
                                     list_length(ind->indirection) - 1);

    // Transform the base expression
    expr = transformExpr(pstate, (Node *) ind, exprKind);

    // Expand the rowtype expression into individual fields
    return ExpandRowReference(pstate, expr, make_target_entry);
}
```