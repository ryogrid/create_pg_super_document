# exprCollation

## Location
[src/backend/nodes/nodeFuncs.c:816-1067](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/nodeFuncs.c#L816-L1067)

## Overview
Returns the OID of the collation associated with an expression's result, handling all PostgreSQL expression node types to determine their appropriate collation properties.

## Definition
```c
Oid exprCollation(const Node *expr)
```

## Detailed Description
This comprehensive function analyzes any PostgreSQL expression node to determine the collation OID of its result value. It implements a large switch statement covering all expression node types, from simple variables and constants to complex subqueries and JSON expressions. The function distinguishes between collatable and non-collatable result types, returning InvalidOid for expressions that produce non-collatable results (like boolean, numeric, or composite types). For collatable expressions, it extracts the appropriate collation from the node's specific collation field. The function handles recursive cases by calling itself on sub-expressions and includes special logic for subqueries, alternative subplans, and JSON expressions.

## Parameters / Member Variables
- `expr`: The expression node to examine for collation information. Can be NULL, in which case InvalidOid is returned.

## Dependencies
- Functions called/Symbols referenced:
  - nodeTag (macro to get node type)
  - IsA (macro for type checking)
  - linitial_node (macro for accessing typed list elements)
  - elog (error logging function)
  - Assert (assertion macro)
  - Various expression node types (Var, Const, Param, FuncExpr, OpExpr, SubLink, etc.)
  - Collation constants (InvalidOid, C_COLLATION_OID, DEFAULT_COLLATION_OID)
  - Sublink type constants (EXPR_SUBLINK, ARRAY_SUBLINK)

- Called from (representative examples):
  - [examine_attribute](examine_attribute.md) (statistics analysis)
  - [create_ctas_nodata](../c/create_ctas_nodata.md) (CREATE TABLE AS operations)
  - [ComputeIndexAttrs](../C/ComputeIndexAttrs.md) (index attribute computation)
  - [ComputePartitionAttrs](../C/ComputePartitionAttrs.md) (partition key analysis)
  - [ExecTypeFromTLInternal](../E/ExecTypeFromTLInternal.md) (executor type handling)
  - [build_pertrans_for_aggref](../b/build_pertrans_for_aggref.md) (aggregate processing)
  - [makeVarFromTargetEntry](../m/makeVarFromTargetEntry.md) (variable creation)
  - [canonicalize_ec_expression](../c/canonicalize_ec_expression.md) (equivalence class processing)
  - [assign_collations_walker](../a/assign_collations_walker.md) (collation assignment)
  - [transformCaseExpr](../t/transformCaseExpr.md) (CASE expression transformation)
  - Self-recursive calls for nested expressions

## Notes and Other Information
- This function is central to PostgreSQL's collation system, which handles locale-specific string comparison and sorting
- The distinction between result collation and input collation is important - this function returns the collation of the expression's output
- Expression nodes that can invoke functions often have separate inputcollid fields for function parameter collation
- Non-collatable result types (boolean, numeric, composite) always return InvalidOid
- Special handling exists for subqueries, where collation comes from the first target list entry
- JSON expressions have complex collation rules depending on whether coercion is applied
- The function includes comprehensive coverage of all PostgreSQL expression node types as of the current version
- Critical for query planning, index operations, and proper string comparison semantics

## Simplified Source

```c
Oid exprCollation(const Node *expr)
{
    Oid coll;

    if (!expr)
        return InvalidOid;

    switch (nodeTag(expr))
    {
        // Basic expression types - extract collation from specific field
        case T_Var:
            coll = ((const Var *) expr)->varcollid;
            break;
        case T_Const:
            coll = ((const Const *) expr)->constcollid;
            break;
        case T_Param:
            coll = ((const Param *) expr)->paramcollid;
            break;

        // Function and operator expressions
        case T_Aggref:
            coll = ((const Aggref *) expr)->aggcollid;
            break;
        case T_WindowFunc:
            coll = ((const WindowFunc *) expr)->wincollid;
            break;
        case T_FuncExpr:
            coll = ((const FuncExpr *) expr)->funccollid;
            break;
        case T_OpExpr:
        case T_DistinctExpr:
        case T_NullIfExpr:
            coll = ((const OpExpr *) expr)->opcollid;
            break;

        // Boolean expressions have no collation
        case T_BoolExpr:
        case T_ScalarArrayOpExpr:
        case T_NullTest:
        case T_BooleanTest:
        case T_CurrentOfExpr:
            coll = InvalidOid;
            break;

        // Subqueries - get collation from first target column
        case T_SubLink:
            {
                const SubLink *sublink = (const SubLink *) expr;
                if (sublink->subLinkType == EXPR_SUBLINK ||
                    sublink->subLinkType == ARRAY_SUBLINK)
                {
                    Query *qtree = (Query *) sublink->subselect;
                    if (!qtree || !IsA(qtree, Query))
                        elog(ERROR, "cannot get collation for untransformed sublink");
                    TargetEntry *tent = linitial_node(TargetEntry, qtree->targetList);
                    coll = exprCollation((Node *) tent->expr);
                }
                else
                    coll = InvalidOid;
            }
            break;

        case T_SubPlan:
            {
                const SubPlan *subplan = (const SubPlan *) expr;
                if (subplan->subLinkType == EXPR_SUBLINK ||
                    subplan->subLinkType == ARRAY_SUBLINK)
                    coll = subplan->firstColCollation;
                else
                    coll = InvalidOid;
            }
            break;

        // Type coercion expressions
        case T_RelabelType:
            coll = ((const RelabelType *) expr)->resultcollid;
            break;
        case T_CoerceViaIO:
            coll = ((const CoerceViaIO *) expr)->resultcollid;
            break;
        case T_ArrayCoerceExpr:
            coll = ((const ArrayCoerceExpr *) expr)->resultcollid;
            break;
        case T_CoerceToDomain:
            coll = ((const CoerceToDomain *) expr)->resultcollid;
            break;

        // Explicit collation
        case T_CollateExpr:
            coll = ((const CollateExpr *) expr)->collOid;
            break;

        // Conditional expressions
        case T_CaseExpr:
            coll = ((const CaseExpr *) expr)->casecollid;
            break;
        case T_CoalesceExpr:
            coll = ((const CoalesceExpr *) expr)->coalescecollid;
            break;
        case T_MinMaxExpr:
            coll = ((const MinMaxExpr *) expr)->minmaxcollid;
            break;

        // Recursive cases
        case T_NamedArgExpr:
            coll = exprCollation((Node *) ((const NamedArgExpr *) expr)->arg);
            break;
        case T_AlternativeSubPlan:
            coll = exprCollation((Node *) linitial(((const AlternativeSubPlan *) expr)->subplans));
            break;
        case T_InferenceElem:
            coll = exprCollation((Node *) ((const InferenceElem *) expr)->expr);
            break;
        case T_PlaceHolderVar:
            coll = exprCollation((Node *) ((const PlaceHolderVar *) expr)->phexpr);
            break;

        // Composite types have no collation
        case T_RowExpr:
        case T_FieldStore:
        case T_ConvertRowtypeExpr:
            coll = InvalidOid;
            break;

        // Special cases with specific logic
        case T_SQLValueFunction:
            if (((const SQLValueFunction *) expr)->type == NAMEOID)
                coll = C_COLLATION_OID;
            else
                coll = InvalidOid;
            break;

        case T_XmlExpr:
            if (((const XmlExpr *) expr)->op == IS_XMLSERIALIZE)
                coll = DEFAULT_COLLATION_OID;
            else
                coll = InvalidOid;
            break;

        // Additional cases for arrays, JSON, etc.
        // ... (many more cases handled similarly)

        default:
            elog(ERROR, "unrecognized node type: %d", (int) nodeTag(expr));
            coll = InvalidOid;
            break;
    }
    return coll;
}
```