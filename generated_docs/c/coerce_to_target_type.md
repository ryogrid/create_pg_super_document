# coerce_to_target_type

## Location
[src/backend/parser/parse_coerce.c:78-156](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_coerce.c#L78-L156)

## Overview
Converts an expression to a target type and typmod, serving as the general-purpose entry point for arbitrary type coercion operations in PostgreSQL's parser.

## Definition

```c
Node *
coerce_to_target_type(ParseState *pstate, Node *expr, Oid exprtype,
					  Oid targettype, int32 targettypmod,
					  CoercionContext ccontext,
					  CoercionForm cformat,
					  int location)
```
## Detailed Description
This function provides a comprehensive type coercion mechanism that attempts to convert an input expression from its current type to a desired target type and typmod. Unlike direct calls to component coercion functions, this function handles the complete coercion pipeline including:

1. **Feasibility Check**: First verifies if the coercion is possible using 
2. **CollateExpr Handling**: Intelligently manages CollateExpr nodes by stripping them before coercion and reinstalling them afterward if the target type is collatable
3. **Type Coercion**: Performs the actual type conversion using 
4. **Typmod Coercion**: Applies additional length/precision coercion using  for fixed-length types

The function returns NULL rather than throwing errors directly, allowing callers to generate custom error messages with appropriate context information.

## Parameters / Member Variables
- `*pstate`: Parse state context (can be NULL, see coerce_type)
- `*expr`: Input expression tree (already transformed by transformExpr)
- `exprtype`: Current result type of the input expression
- `targettype`: Desired result type for the coercion
- `targettypmod`: Desired result typmod for the coercion
- `ccontext`: Coercion context indicating the circumstances of the coercion
- `cformat`: Coercion format controlling how the coercion is displayed
- `location`: Parse location of the coercion request, or -1 if unknown/implicit
## Dependencies
- Functions called/Symbols referenced:
  - [can_coerce_type](can_coerce_type.md)
  - [coerce_type](coerce_type.md)
  - [coerce_type_typmod](coerce_type_typmod.md)
  - [type_is_collatable](../t/type_is_collatable.md)
  - [CollateExpr](../C/CollateExpr.md) (node type)
  - CoercionContext (enum)
  - CoercionForm (enum)
- Called from (representative examples):
  - [transformTypeCast](../t/transformTypeCast.md)
  - [transformAssignedExpr](../t/transformAssignedExpr.md)
  - [coerce_to_boolean](coerce_to_boolean.md)
  - [build_coercion_expression](../b/build_coercion_expression.md)
  - [ATExecAlterColumnType](../A/ATExecAlterColumnType.md)

## Notes and Other Information
- This is the recommended entry point for type coercion operations; direct use of component functions should be limited to special cases
- The function carefully manages CollateExpr nodes to preserve collation information through the coercion process
- For fixed-length types requiring both type and length coercion, the inner coercion node is forced to implicit display form
- Returns NULL on coercion failure rather than reporting errors, enabling context-specific error handling by callers
- Located in src/backend/parser/parse_coerce.c:78-156

## Simplified Source

```c
Node *coerce_to_target_type(ParseState *pstate, Node *expr, Oid exprtype,
                          Oid targettype, int32 targettypmod,
                          CoercionContext ccontext,
                          CoercionForm cformat,
                          int location) {
    // Check if coercion is possible before attempting
    if (!can_coerce_type(1, &exprtype, &targettype, ccontext))
        return NULL;

    // Handle CollateExpr: strip it off before coercion
    Node *origexpr = expr;
    while (expr && IsA(expr, CollateExpr))
        expr = (Node *) ((CollateExpr *) expr)->arg;

    // Perform the main type coercion
    Node *result = coerce_type(pstate, expr, exprtype,
                              targettype, targettypmod,
                              ccontext, cformat, location);

    // Apply additional length/precision coercion for fixed-length types
    result = coerce_type_typmod(result,
                               targettype, targettypmod,
                               ccontext, cformat, location,
                               (result != expr && !IsA(result, Const)));

    // Reinstall CollateExpr if original had one and target type is collatable
    if (expr != origexpr && type_is_collatable(targettype)) {
        CollateExpr *coll = (CollateExpr *) origexpr;
        CollateExpr *newcoll = makeNode(CollateExpr);

        newcoll->arg = (Expr *) result;
        newcoll->collOid = coll->collOid;
        newcoll->location = coll->location;
        result = (Node *) newcoll;
    }

    return result;
}
```