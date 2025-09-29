# exprTypmod

## Location
[src/backend/nodes/nodeFuncs.c:298-551](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L298-L551)

## Overview
Returns the type-specific modifier (typmod) of an expression's result type, if it can be determined, otherwise returns -1.

## Definition

```c
structorExpr:
			return ((const JsonConstructorExpr *) expr)->returning->typmod;
```
## Detailed Description
The  function extracts the type modifier information from PostgreSQL expression nodes. Type modifiers provide additional constraints on data types, such as precision and scale for numeric types, length for character types, or other type-specific parameters.

Unlike  which can always determine a type,  often returns -1 when the type modifier cannot be determined or is not meaningful for the expression type. The function handles various expression types:

- For basic expressions (Var, Const, Param), it returns the stored typmod directly
- For function expressions, it attempts to detect length-coercion functions using 
- For complex expressions like CASE, COALESCE, ARRAY, and MIN/MAX, it checks if all alternatives have the same typmod and returns it, otherwise returns -1
- For subqueries, it recursively determines the typmod of the first target column
- For type coercion expressions, it returns the result typmod

The function implements a conservative approach: when there's any ambiguity about the typmod, it returns -1 rather than making assumptions.

## Parameters / Member Variables
- : A const pointer to the Node representing the expression whose type modifier should be determined. If NULL, returns -1.

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (to determine expression node type)
  - [exprIsLengthCoercion](exprIsLengthCoercion.md) (to detect length-coercion functions)
  - [exprTypmod](exprTypmod.md) (recursive calls for nested expressions)
  - [exprType](exprType.md) (for type checking in complex expressions)
  - linitial, linitial_node (for accessing list elements)
  - lfirst, lfirst_node (for list iteration)
  - for_each_from (for iterating from specific list positions)

- Called from (representative examples):
  - Type coercion and checking functions (coerce_type_typmod, select_common_typmod)
  - [Query](../Q/Query.md) planning and optimization (set_rel_width, get_expr_width)
  - Tuple descriptor construction (ConstructTupleDescriptor, ExecTypeFromTLInternal)
  - Parser functions for expressions and target lists

## Notes and Other Information
- Returns -1 for NULL input expressions or when typmod cannot be determined
- For length-coercion functions, uses  to extract the coerced typmod
- [Complex](../C/Complex.md) expressions (CASE, COALESCE, ARRAY, MIN/MAX) require all alternatives to agree on typmod
- For subqueries, array vs. non-array distinction doesn't affect typmod handling
- Type modifiers are crucial for proper type coercion and storage decisions
- The function is widely used throughout PostgreSQL's type system, particularly in query planning and execution
- Located in src/backend/nodes/nodeFuncs.c:298-551

## Simplified Source

```c
int32 exprTypmod(const Node *expr)
{
    if (!expr)
        return -1;

    switch (nodeTag(expr))
    {
        // Basic expression types - return stored typmod
        case T_Var:
            return ((const Var *) expr)->vartypmod;
        case T_Const:
            return ((const Const *) expr)->consttypmod;
        case T_Param:
            return ((const Param *) expr)->paramtypmod;
        case T_SubscriptingRef:
            return ((const SubscriptingRef *) expr)->reftypmod;

        // Function expressions - check for length coercion
        case T_FuncExpr:
            {
                int32 coercedTypmod;
                if (exprIsLengthCoercion(expr, &coercedTypmod))
                    return coercedTypmod;
            }
            break;

        // Recursive cases
        case T_NamedArgExpr:
            return exprTypmod((Node *) ((const NamedArgExpr *) expr)->arg);
        case T_CollateExpr:
            return exprTypmod((Node *) ((const CollateExpr *) expr)->arg);
        case T_PlaceHolderVar:
            return exprTypmod((Node *) ((const PlaceHolderVar *) expr)->phexpr);

        // Special handling for NULLIF
        case T_NullIfExpr:
            {
                const NullIfExpr *nexpr = (const NullIfExpr *) expr;
                return exprTypmod((Node *) linitial(nexpr->args));
            }
            break;

        // Subqueries - get typmod from first target column
        case T_SubLink:
            {
                const SubLink *sublink = (const SubLink *) expr;
                if (sublink->subLinkType == EXPR_SUBLINK ||
                    sublink->subLinkType == ARRAY_SUBLINK)
                {
                    Query *qtree = (Query *) sublink->subselect;
                    if (!qtree || !IsA(qtree, Query))
                        elog(ERROR, "cannot get type for untransformed sublink");
                    TargetEntry *tent = linitial_node(TargetEntry, qtree->targetList);
                    return exprTypmod((Node *) tent->expr);
                }
            }
            break;

        case T_SubPlan:
            {
                const SubPlan *subplan = (const SubPlan *) expr;
                if (subplan->subLinkType == EXPR_SUBLINK ||
                    subplan->subLinkType == ARRAY_SUBLINK)
                    return subplan->firstColTypmod;
            }
            break;

        case T_AlternativeSubPlan:
            {
                const AlternativeSubPlan *asplan = (const AlternativeSubPlan *) expr;
                return exprTypmod((Node *) linitial(asplan->subplans));
            }
            break;

        // Type coercion expressions
        case T_FieldSelect:
            return ((const FieldSelect *) expr)->resulttypmod;
        case T_RelabelType:
            return ((const RelabelType *) expr)->resulttypmod;
        case T_ArrayCoerceExpr:
            return ((const ArrayCoerceExpr *) expr)->resulttypmod;
        case T_CoerceToDomain:
            return ((const CoerceToDomain *) expr)->resulttypmod;

        // Test expressions
        case T_CaseTestExpr:
            return ((const CaseTestExpr *) expr)->typeMod;
        case T_CoerceToDomainValue:
            return ((const CoerceToDomainValue *) expr)->typeMod;
        case T_SetToDefault:
            return ((const SetToDefault *) expr)->typeMod;

        // Complex expressions - all alternatives must agree on typmod
        case T_CaseExpr:
            {
                const CaseExpr *cexpr = (const CaseExpr *) expr;
                Oid casetype = cexpr->casetype;
                int32 typmod;
                ListCell *arg;

                if (!cexpr->defresult)
                    return -1;
                if (exprType((Node *) cexpr->defresult) != casetype)
                    return -1;
                typmod = exprTypmod((Node *) cexpr->defresult);
                if (typmod < 0)
                    return -1;

                foreach(arg, cexpr->args)
                {
                    CaseWhen *w = lfirst_node(CaseWhen, arg);
                    if (exprType((Node *) w->result) != casetype)
                        return -1;
                    if (exprTypmod((Node *) w->result) != typmod)
                        return -1;
                }
                return typmod;
            }
            break;

        case T_ArrayExpr:
            {
                const ArrayExpr *arrayexpr = (const ArrayExpr *) expr;
                Oid commontype;
                int32 typmod;
                ListCell *elem;

                if (arrayexpr->elements == NIL)
                    return -1;
                typmod = exprTypmod((Node *) linitial(arrayexpr->elements));
                if (typmod < 0)
                    return -1;

                commontype = arrayexpr->multidims ?
                            arrayexpr->array_typeid :
                            arrayexpr->element_typeid;

                foreach(elem, arrayexpr->elements)
                {
                    Node *e = (Node *) lfirst(elem);
                    if (exprType(e) != commontype)
                        return -1;
                    if (exprTypmod(e) != typmod)
                        return -1;
                }
                return typmod;
            }
            break;

        // Similar logic for COALESCE and MIN/MAX...
        case T_CoalesceExpr:
        case T_MinMaxExpr:
            // Implementation similar to CaseExpr - check all args agree
            // ... (detailed implementation omitted for brevity)
            break;

        // SQL value functions and JSON expressions
        case T_SQLValueFunction:
            return ((const SQLValueFunction *) expr)->typmod;
        case T_JsonValueExpr:
            return exprTypmod((Node *) ((const JsonValueExpr *) expr)->formatted_expr);
        case T_JsonConstructorExpr:
            return ((const JsonConstructorExpr *) expr)->returning->typmod;
        case T_JsonExpr:
            return ((const JsonExpr *) expr)->returning->typmod;
        case T_JsonBehavior:
            return exprTypmod(((const JsonBehavior *) expr)->expr);

        default:
            break;
    }
    return -1;  // Cannot determine typmod
}
```