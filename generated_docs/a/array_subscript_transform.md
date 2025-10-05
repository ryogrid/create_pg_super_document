# array_subscript_transform

## Location
[src/backend/utils/adt/arraysubs.c:55-179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/arraysubs.c#L55-L179)

## Overview
Completes parse analysis of a SubscriptingRef expression for an array by transforming subscript expressions, coercing them to integers, and determining the result type of the SubscriptingRef node.

## Definition

```c
static void
array_subscript_transform(SubscriptingRef *sbsref,
						  List *indirection,
						  ParseState *pstate,
						  bool isSlice,
						  bool isAssignment)
```
## Detailed Description
This function is a critical part of PostgreSQL's array subscripting infrastructure during parse analysis. It processes the raw subscript expressions from the parser and transforms them into a form suitable for execution. The function handles both single-element access and array slicing operations.

The transformation process involves:
1. Iterating through each subscript expression in the indirection list
2. Transforming subscript expressions using the parse state
3. Coercing all subscript values to INT4OID (integer) type
4. Separating upper and lower bounds for slice operations
5. Handling special cases like omitted bounds in slices
6. Setting the appropriate result type based on whether the operation is a slice or element access

For slice operations, non-slice indirection items are converted to slices by treating the single subscript as the upper bound and supplying an assumed lower bound of 1. The function also enforces PostgreSQL's maximum dimension limit (MAXDIM).

## Parameters / Member Variables
- `*sbsref`: The SubscriptingRef node being transformed, which will be updated with the processed subscript expressions and result type
- `*indirection`: List of A_Indices structures representing the raw subscript expressions from the parser
- `*pstate`: Parse state containing context information needed for expression transformation
- `isSlice`: Boolean indicating whether this is a slice operation (affects result type determination)
- `isAssignment`: Boolean indicating whether this subscripting is part of an assignment operation
## Dependencies
- Functions called/Symbols referenced:
  - [transformExpr](../t/transformExpr.md) (transforms raw expressions into executable form)
  - [coerce_to_target_type](../c/coerce_to_target_type.md) (coerces expressions to INT4OID type)
  - [exprType](../e/exprType.md) (determines the type of an expression)
  - [makeConst](../m/makeConst.md) (creates constant expressions for default lower bounds)
  - [exprLocation](../e/exprLocation.md) (gets source location for error reporting)
  - [lappend](../l/lappend.md) (appends to PostgreSQL lists)
  - ereport/lfirst_node (error reporting and list manipulation)
- Called from (representative examples):
  - [array_subscript_handler](array_subscript_handler.md) (main array subscript handler)
  - [raw_array_subscript_handler](../r/raw_array_subscript_handler.md) (raw array subscript handler)

## Notes and Other Information
- This is a static function internal to the array subscripting module
- Enforces type safety by requiring all subscripts to be coercible to integers
- Supports PostgreSQL's array slicing syntax with omitted bounds
- Part of the subscripting framework introduced to support custom subscripting for different data types
- Maximum array dimensions are limited by MAXDIM constant
- Error messages provide parser position information for better user experience
- The function modifies the SubscriptingRef node in-place rather than returning a new structure

## Simplified Source

```c
static void
array_subscript_transform(SubscriptingRef *sbsref,
                          List *indirection,
                          ParseState *pstate,
                          bool isSlice,
                          bool isAssignment)
{
    List *upperIndexpr = NIL;
    List *lowerIndexpr = NIL;
    ListCell *idx;

    // Transform subscript expressions and separate upper/lower bounds
    foreach(idx, indirection) {
        A_Indices *ai = lfirst_node(A_Indices, idx);
        Node *subexpr;

        // Handle lower bounds for slice operations
        if (isSlice) {
            if (ai->lidx) {
                // Transform and coerce lower bound to integer
                subexpr = transformExpr(pstate, ai->lidx, pstate->p_expr_kind);
                subexpr = coerce_to_target_type(pstate, subexpr, exprType(subexpr),
                                               INT4OID, -1, COERCION_ASSIGNMENT,
                                               COERCE_IMPLICIT_CAST, -1);
                if (subexpr == NULL)
                    ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                                   errmsg("array subscript must have type integer")));
            } else if (!ai->is_slice) {
                // Default lower bound to 1 for non-slice items
                subexpr = (Node *) makeConst(INT4OID, -1, InvalidOid, sizeof(int32),
                                           Int32GetDatum(1), false, true);
            } else {
                // Omitted lower bound in slice
                subexpr = NULL;
            }
            lowerIndexpr = lappend(lowerIndexpr, subexpr);
        }

        // Handle upper bounds
        if (ai->uidx) {
            // Transform and coerce upper bound to integer
            subexpr = transformExpr(pstate, ai->uidx, pstate->p_expr_kind);
            subexpr = coerce_to_target_type(pstate, subexpr, exprType(subexpr),
                                           INT4OID, -1, COERCION_ASSIGNMENT,
                                           COERCE_IMPLICIT_CAST, -1);
            if (subexpr == NULL)
                ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                               errmsg("array subscript must have type integer")));
        } else {
            // Omitted upper bound in slice
            subexpr = NULL;
        }
        upperIndexpr = lappend(upperIndexpr, subexpr);
    }

    // Store transformed expressions in SubscriptingRef node
    sbsref->refupperindexpr = upperIndexpr;
    sbsref->reflowerindexpr = lowerIndexpr;

    // Check dimension limits
    if (list_length(upperIndexpr) > MAXDIM)
        ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                       errmsg("number of array dimensions (%d) exceeds the maximum allowed (%d)",
                              list_length(upperIndexpr), MAXDIM)));

    // Set result type: same as container for slices, element type for single access
    if (isSlice)
        sbsref->refrestype = sbsref->refcontainertype;
    else
        sbsref->refrestype = sbsref->refelemtype;
}
```