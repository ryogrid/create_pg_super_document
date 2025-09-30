# transformArrayExpr

## Location
[src/backend/parser/parse_expr.c:2015-2175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L2015-L2175)

## Overview
Transforms an array expression from parse tree format (A_ArrayExpr) to executable format (ArrayExpr), handling type inference, element coercion, and multi-dimensional array detection.

## Definition
```c
static Node *
transformArrayExpr(ParseState *pstate, A_ArrayExpr *a,
                   Oid array_type, Oid element_type, int32 typmod)
```

## Detailed Description
The `transformArrayExpr` function is responsible for converting parsed array expressions into their executable representation during the semantic analysis phase of query processing. It performs several critical tasks:

1. **Element Transformation**: Recursively transforms each element expression in the array, handling nested arrays by recursive calls to itself
2. **Multi-dimensional Detection**: Automatically detects whether the array is multi-dimensional by checking if any elements are themselves arrays (excluding special vector types like INT2VECTOROID and OIDVECTOROID)
3. **Type Inference**: If no target type is specified, uses `select_common_type()` to deduce the most appropriate common type for all elements
4. **Type Coercion**: Applies appropriate type coercion to all elements, using explicit coercion when a target type is provided, or implicit coercion when inferring the common type
5. **Error Handling**: Provides detailed error messages for cases like empty arrays without type specification or failed type coercions

The function handles both simple one-dimensional arrays and complex multi-dimensional arrays, ensuring type consistency across all elements while maintaining the original location information for error reporting.

## Parameters / Member Variables
- `pstate`: Parse state containing context information for the current parsing operation
- `a`: The raw array expression from the parse tree (A_ArrayExpr) to be transformed
- `array_type`: Optional target array type OID; if valid, forces elements to this array type
- `element_type`: Optional target element type OID; used when array_type is specified
- `typmod`: Type modifier for the target type, providing additional type constraints

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates new ArrayExpr node)
  - [transformExprRecurse](transformExprRecurse.md) (transforms individual element expressions)
  - [transformArrayExpr](transformArrayExpr.md) (recursive call for nested arrays)
  - [select_common_type](../s/select_common_type.md) (infers common type from multiple expressions)
  - [get_element_type](../g/get_element_type.md) (extracts element type from array type)
  - [get_array_type](../g/get_array_type.md) (finds array type for given element type)
  - [coerce_to_target_type](../c/coerce_to_target_type.md) (explicit type coercion)
  - [coerce_to_common_type](../c/coerce_to_common_type.md) (implicit type coercion)
  - type_is_array (checks if type is an array type)
  - [exprType](../e/exprType.md) (gets the type of an expression)
  - [exprLocation](../e/exprLocation.md) (gets source location of an expression)

- Called from (representative examples):
  - [transformExprRecurse](transformExprRecurse.md) (main expression transformation dispatcher)
  - [transformTypeCast](transformTypeCast.md) (when casting to array types)

## Notes and Other Information
- The function sets `multidims = true` when it detects array elements, enabling proper handling of multi-dimensional arrays
- Special vector types (INT2VECTOROID, OIDVECTOROID) are not treated as sub-arrays due to their special constraints
- Empty arrays require explicit type casting (e.g., ARRAY[]::integer[]) since type inference is impossible
- The function preserves the original source location for accurate error reporting
- Type coercion behavior differs based on whether the target type was explicitly specified (hard coercion) or inferred (soft coercion)
- The resulting ArrayExpr node will have its array_collid set later by the collation analysis phase

## Simplified Source

```c
static Node *
transformArrayExpr(ParseState *pstate, A_ArrayExpr *a,
                   Oid array_type, Oid element_type, int32 typmod)
{
    ArrayExpr *newa = makeNode(ArrayExpr);
    List *newelems = NIL;
    List *newcoercedelems = NIL;
    Oid coerce_type;
    bool coerce_hard;

    // Transform all elements, detecting multi-dimensional arrays
    newa->multidims = false;
    foreach(element, a->elements) {
        Node *e = lfirst(element);
        Node *newe;

        if (IsA(e, A_ArrayExpr)) {
            // Recursive transformation for nested arrays
            newe = transformArrayExpr(pstate, (A_ArrayExpr *) e,
                                      array_type, element_type, typmod);
            newa->multidims = true;
        } else {
            newe = transformExprRecurse(pstate, e);

            // Check if element is an array type (indicates multi-dimensional)
            if (!newa->multidims) {
                Oid newetype = exprType(newe);
                if (newetype != INT2VECTOROID && newetype != OIDVECTOROID &&
                    type_is_array(newetype))
                    newa->multidims = true;
            }
        }
        newelems = lappend(newelems, newe);
    }

    // Determine target type for elements
    if (OidIsValid(array_type)) {
        // Explicit target type provided
        coerce_type = (newa->multidims ? array_type : element_type);
        coerce_hard = true;
    } else {
        // Infer common type from elements
        if (newelems == NIL)
            ereport(ERROR, (errcode(ERRCODE_INDETERMINATE_DATATYPE),
                            errmsg("cannot determine type of empty array"),
                            errhint("Explicitly cast to the desired type, "
                                    "for example ARRAY[]::integer[]."),
                            parser_errposition(pstate, a->location)));

        coerce_type = select_common_type(pstate, newelems, "ARRAY", NULL);

        if (newa->multidims) {
            array_type = coerce_type;
            element_type = get_element_type(array_type);
        } else {
            element_type = coerce_type;
            array_type = get_array_type(element_type);
        }
        coerce_hard = false;
    }

    // Coerce all elements to target type
    foreach(element, newelems) {
        Node *e = lfirst(element);
        Node *newe;

        if (coerce_hard) {
            newe = coerce_to_target_type(pstate, e, exprType(e), coerce_type,
                                         typmod, COERCION_EXPLICIT,
                                         COERCE_EXPLICIT_CAST, -1);
        } else {
            newe = coerce_to_common_type(pstate, e, coerce_type, "ARRAY");
        }
        newcoercedelems = lappend(newcoercedelems, newe);
    }

    // Set final array properties
    newa->array_typeid = array_type;
    newa->element_typeid = element_type;
    newa->elements = newcoercedelems;
    newa->location = a->location;

    return (Node *) newa;
}
```