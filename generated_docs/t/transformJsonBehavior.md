# transformJsonBehavior

## Location
[src/backend/parser/parse_expr.c:4696-4834](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_expr.c#L4696-L4834)

## Overview
Transforms a JSON BEHAVIOR clause during SQL parsing, handling type coercion and validation for JSON function expressions.

## Definition
```c
static JsonBehavior *
transformJsonBehavior(ParseState *pstate, JsonBehavior *behavior,
                      JsonBehaviorType default_behavior,
                      JsonReturning *returning)
```

## Detailed Description
This function processes JSON BEHAVIOR clauses used in SQL/JSON functions, applying necessary transformations and validations. It handles different behavior types (NULL, ERROR, DEFAULT, EMPTY ARRAY, etc.) and performs type coercion between the behavior expression and the target return type. The function validates DEFAULT expressions to ensure they are constants, non-aggregate functions, or operators without column references or set-returning capabilities.

The function performs runtime coercion using json_populate_type() for NULL, jsonb-valued, or boolean-valued expressions (except when targeting integer types). For other expressions, it attempts to find appropriate cast functions and reports errors if coercion is not possible.

## Parameters / Member Variables
- `pstate`: ParseState context for error reporting and expression transformation
- `behavior`: Input JsonBehavior node to transform (can be NULL for default behavior)
- `default_behavior`: Default JsonBehaviorType to use when behavior is NULL
- `returning`: JsonReturning structure containing target type information for coercion

## Dependencies
- Functions called/Symbols referenced:
  - [transformExprRecurse](transformExprRecurse.md)
  - [ValidJsonBehaviorDefaultExpr](../V/ValidJsonBehaviorDefaultExpr.md)  
  - [GetJsonBehaviorConst](../G/GetJsonBehaviorConst.md)
  - [contain_var_clause](../c/contain_var_clause.md)
  - [expression_returns_set](../e/expression_returns_set.md)
  - [exprType](../e/exprType.md)
  - [getBaseType](../g/getBaseType.md)
  - [TypeCategory](../T/TypeCategory.md)
  - [coerce_to_target_type](../c/coerce_to_target_type.md)
  - [makeConst](../m/makeConst.md)
  - [makeJsonBehavior](../m/makeJsonBehavior.md)
  - DirectFunctionCall1
  - [jsonb_in](../j/jsonb_in.md)
- Called from (representative examples):
  - [transformJsonFuncExpr](transformJsonFuncExpr.md)

## Notes and Other Information
The function performs extensive validation for DEFAULT behavior expressions, ensuring they do not contain column references or return sets. It uses different coercion strategies based on the source and target types, with special handling for string types using assignment casts to preserve length constraints. The coerce_at_runtime flag is set when json_populate_type() should be used for runtime type conversion.

## Simplified Source

```c
static JsonBehavior *
transformJsonBehavior(ParseState *pstate, JsonBehavior *behavior,
                      JsonBehaviorType default_behavior,
                      JsonReturning *returning) {
    JsonBehaviorType btype = default_behavior;
    Node *expr = NULL;
    bool coerce_at_runtime = false;
    int location = -1;

    // Extract behavior type and location if provided
    if (behavior) {
        btype = behavior->btype;
        location = behavior->location;

        if (btype == JSON_BEHAVIOR_DEFAULT) {
            // Transform and validate DEFAULT expression
            expr = transformExprRecurse(pstate, behavior->expr);

            // Validate DEFAULT expression constraints
            if (!ValidJsonBehaviorDefaultExpr(expr, NULL) ||
                contain_var_clause(expr) ||
                expression_returns_set(expr)) {
                // Report appropriate error with location
                ereport(ERROR, ...);
            }
        }
    }

    // Generate constant expression for non-DEFAULT behaviors
    if (expr == NULL && btype != JSON_BEHAVIOR_ERROR)
        expr = GetJsonBehaviorConst(btype, location);

    // Handle type coercion if needed
    if (expr && exprType(expr) != returning->typid) {
        bool isnull = (IsA(expr, Const) && ((Const *) expr)->constisnull);

        if (isnull || exprType(expr) == JSONBOID ||
            (exprType(expr) == BOOLOID && getBaseType(returning->typid) != INT4OID)) {
            // Use runtime coercion via json_populate_type()
            coerce_at_runtime = true;

            // Convert boolean to jsonb constant if needed
            if (exprType(expr) == BOOLOID) {
                char *val = btype == JSON_BEHAVIOR_TRUE ? "true" : "false";
                expr = (Node *) makeConst(JSONBOID, -1, InvalidOid, -1,
                                        DirectFunctionCall1(jsonb_in, CStringGetDatum(val)),
                                        false, false);
            }
        } else {
            // Attempt explicit coercion
            Node *coerced_expr = coerce_to_target_type(pstate, expr, exprType(expr),
                                                     returning->typid, returning->typmod,
                                                     /* coercion_context */,
                                                     COERCE_EXPLICIT_CAST,
                                                     exprLocation((Node *) behavior));

            if (coerced_expr == NULL) {
                // Report coercion failure with helpful hints
                ereport(ERROR, ...);
            }
            expr = coerced_expr;
        }
    }

    // Create or update behavior node
    if (behavior)
        behavior->expr = expr;
    else
        behavior = makeJsonBehavior(btype, expr, location);

    behavior->coerce = coerce_at_runtime;
    return behavior;
}
```