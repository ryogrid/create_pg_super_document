# jsonb_subscript_transform

## Location
[src/backend/utils/adt/jsonbsubs.c:43-174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonbsubs.c#L43-L174)

## Overview
Finishes parse analysis of a SubscriptingRef expression for JSONB by transforming subscript expressions, coercing them to appropriate types, and determining the result type.

## Definition

```c
static void
jsonb_subscript_transform(SubscriptingRef *sbsref,
						  List *indirection,
						  ParseState *pstate,
						  bool isSlice,
						  bool isAssignment)
```
## Detailed Description
This function handles the transformation phase of JSONB subscripting operations during SQL parsing. It processes each subscript expression in the indirection list, validates that slicing is not used (which is unsupported for JSONB), and coerces subscript expressions to either integer or text types. The function implements type disambiguation logic to ensure that subscripts can only be coerced to one target type, preventing ambiguous subscript operations similar to overloaded function resolution.

The transformation process includes:
1. Iterating through all subscript expressions in the indirection list
2. Rejecting slice operations with appropriate error messages
3. Determining whether each subscript can be coerced to int4 or text
4. Ensuring no ambiguous coercion scenarios (subscript coercible to multiple types)
5. Performing the actual type coercion
6. Setting the result type to JSONBOID

## Parameters / Member Variables
- `*sbsref`: The SubscriptingRef node being transformed, which will be updated with processed subscript expressions
- `*indirection`: List of A_Indices nodes representing the subscript expressions to be processed
- `*pstate`: Parse state containing context information for error reporting and expression transformation
- `isSlice`: Boolean indicating if this is a slice operation (always results in error for JSONB)
- `isAssignment`: Boolean indicating if this subscripting is part of an assignment operation
## Dependencies
- Functions called/Symbols referenced:
  - [transformExpr](../t/transformExpr.md)
  - [exprType](../e/exprType.md)
  - [can_coerce_type](../c/can_coerce_type.md)
  - [coerce_type](../c/coerce_type.md)
  - [format_type_be](../f/format_type_be.md)
  - [exprLocation](../e/exprLocation.md)
  - ereport
- Called from:
  - [jsonb_subscript_handler](jsonb_subscript_handler.md)

## Notes and Other Information
- JSONB subscripting does not support slice operations and will generate errors if attempted
- Subscripts must be coercible to either integer (for array indexing) or text (for object key access)
- The function implements strict type disambiguation - if a subscript type can be coerced to both int4 and text, it generates an error
- Uses implicit coercion context similar to overloaded function resolution
- Always sets the result type to JSONBOID regardless of the subscript types used
- Error messages include parser position information for better user experience

## Simplified Source

```c
static void jsonb_subscript_transform(SubscriptingRef *sbsref,
                                    List *indirection,
                                    ParseState *pstate,
                                    bool isSlice,
                                    bool isAssignment) {
    List *upperIndexpr = NIL;
    ListCell *idx;

    // Process each subscript expression
    foreach(idx, indirection) {
        A_Indices *ai = lfirst_node(A_Indices, idx);
        Node *subExpr;

        // JSONB doesn't support slicing operations
        if (isSlice) {
            Node *expr = ai->uidx ? ai->uidx : ai->lidx;
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("jsonb subscript does not support slices"),
                 parser_errposition(pstate, exprLocation(expr))));
        }

        if (ai->uidx) {
            Oid subExprType = InvalidOid, targetType = UNKNOWNOID;

            // Transform the subscript expression
            subExpr = transformExpr(pstate, ai->uidx, pstate->p_expr_kind);
            subExprType = exprType(subExpr);

            if (subExprType != UNKNOWNOID) {
                Oid targets[2] = {INT4OID, TEXTOID};

                // Check which target types are possible (int or text)
                for (int i = 0; i < 2; i++) {
                    if (can_coerce_type(1, &subExprType, &targets[i], COERCION_IMPLICIT)) {
                        // Error if already found a valid target (ambiguous)
                        if (targetType != UNKNOWNOID) {
                            ereport(ERROR,
                                (errcode(ERRCODE_DATATYPE_MISMATCH),
                                 errmsg("subscript type %s is not supported", format_type_be(subExprType)),
                                 errhint("jsonb subscript must be coercible to only one type, integer or text."),
                                 parser_errposition(pstate, exprLocation(subExpr))));
                        }
                        targetType = targets[i];
                    }
                }

                // No valid target type found
                if (targetType == UNKNOWNOID) {
                    ereport(ERROR,
                        (errcode(ERRCODE_DATATYPE_MISMATCH),
                         errmsg("subscript type %s is not supported", format_type_be(subExprType)),
                         errhint("jsonb subscript must be coercible to either integer or text."),
                         parser_errposition(pstate, exprLocation(subExpr))));
                }
            } else {
                targetType = TEXTOID;  // Default to text for unknown types
            }

            // Perform type coercion
            subExpr = coerce_type(pstate, subExpr, subExprType,
                                targetType, -1,
                                COERCION_IMPLICIT,
                                COERCE_IMPLICIT_CAST, -1);

            if (subExpr == NULL) {
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                     errmsg("jsonb subscript must have text type"),
                     parser_errposition(pstate, exprLocation(subExpr))));
            }
        } else {
            // Handle missing upper bound (should not happen due to slice check)
            ereport(ERROR,
                (errcode(ERRCODE_DATATYPE_MISMATCH),
                 errmsg("jsonb subscript does not support slices"),
                 parser_errposition(pstate, exprLocation(ai->uidx))));
        }

        upperIndexpr = lappend(upperIndexpr, subExpr);
    }

    // Set the transformed subscript expressions and result type
    sbsref->refupperindexpr = upperIndexpr;
    sbsref->reflowerindexpr = NIL;
    sbsref->refrestype = JSONBOID;
    sbsref->reftypmod = -1;
}
```