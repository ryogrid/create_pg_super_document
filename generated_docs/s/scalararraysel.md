# scalararraysel

## Location
[src/backend/utils/adt/selfuncs.c:1817-2139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L1817-L2139)

## Overview
Computes the selectivity of ScalarArrayOpExpr nodes, handling SQL operations like 'value = ANY(array)' and 'value <> ALL(array)' with sophisticated array analysis.

## Definition

```c
struct the expression */
	Assert(list_length(clause->args) == 2);
```
## Detailed Description
The  function estimates selectivity for scalar array operations, which are SQL expressions comparing a scalar value against an array using operators like ANY or ALL. Examples include 'column = ANY(ARRAY[1,2,3])' or 'value <> ALL(array_column)'.

The function implements a sophisticated multi-tiered approach:

1. **Array Containment Optimization**: For equality/inequality operations, it first attempts to use array containment analysis via , treating expressions like 'const = ANY(column)' as 'ARRAY[const] <@ column' for more accurate estimates.

2. **Constant Array Analysis**: When the array is a constant, it deconstructs the array elements and applies the operator's selectivity function to each element. It uses two probability models:
   - **Independent probabilities**: Standard assumption for generic operators
   - **Disjoint probabilities**: For equality/inequality with distinct elements, probabilities are summed rather than combined independently

3. **ArrayExpr Analysis**: When the array is constructed using ARRAY[] syntax, it processes each element expression individually, applying similar probability combination logic.

4. **Fallback Estimation**: When the array structure is unknown, it creates a dummy element and assumes approximately 10 elements in the array for estimation purposes.

The function handles both OR semantics (ANY operations) and AND semantics (ALL operations), with appropriate probability combination formulas for each case.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : ScalarArrayOpExpr node representing the scalar-array operation
- : Boolean indicating if this is part of a join condition
- : Relation ID to restrict analysis to (0 if no restriction)
- : Type of join operation context
- : Special join information for outer joins

## Dependencies
- Functions called/Symbols referenced:
  - [estimate_expression_value](../e/estimate_expression_value.md)
  - [get_base_element_type](../g/get_base_element_type.md)
  - [strip_array_coercion](strip_array_coercion.md)
  - [scalararraysel_containment](scalararraysel_containment.md)
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - [get_oprjoin](../g/get_oprjoin.md)/get_oprrest
  - [deconstruct_array](../d/deconstruct_array.md)
  - CLAMP_PROBABILITY
- Called from (representative examples):
  - [clause_selectivity_ext](../c/clause_selectivity_ext.md)
  - [GenericCosts](../G/GenericCosts.md)

## Notes and Other Information
- Handles both ANY (OR) and ALL (AND) array operations with appropriate probability mathematics
- Uses sophisticated disjoint probability analysis for equality operations with distinct array elements
- Preprocesses expressions to remove binary-compatible type coercions using strip_array_coercion
- Falls back through multiple analysis strategies based on array expression complexity
- Critical for optimizing queries with IN clauses and array operations
- Assumes 10 elements for unknown array sizes (also used in estimate_array_length)
- Supports both constant arrays and dynamic ARRAY[] constructs
- Ensures final selectivity values are clamped to valid probability range [0.0, 1.0]

## Simplified Source

```c
Selectivity
scalararraysel(PlannerInfo *root, ScalarArrayOpExpr *clause, bool is_join_clause,
               int varRelid, JoinType jointype, SpecialJoinInfo *sjinfo)
{
    Oid operator = clause->opno;
    bool useOr = clause->useOr;
    bool isEquality = false;
    bool isInequality = false;
    Node *leftop, *rightop;
    Oid nominal_element_type, nominal_element_collation;
    TypeCacheEntry *typentry;
    RegProcedure oprsel;
    FmgrInfo oprselproc;
    Selectivity s1;

    // Extract and simplify arguments
    Assert(list_length(clause->args) == 2);
    leftop = (Node *) linitial(clause->args);
    rightop = (Node *) lsecond(clause->args);

    leftop = estimate_expression_value(root, leftop);
    rightop = estimate_expression_value(root, rightop);

    // Get array element type information
    nominal_element_type = get_base_element_type(exprType(rightop));
    if (!OidIsValid(nominal_element_type)) {
        return (Selectivity) 0.5;
    }
    nominal_element_collation = exprCollation(rightop);
    rightop = strip_array_coercion(rightop);

    // Detect equality/inequality operators
    typentry = lookup_type_cache(nominal_element_type, TYPECACHE_EQ_OPR);
    if (OidIsValid(typentry->eq_opr)) {
        if (operator == typentry->eq_opr) {
            isEquality = true;
        } else if (get_negator(operator) == typentry->eq_opr) {
            isInequality = true;
        }
    }

    // Try array containment optimization for equality/inequality
    if ((isEquality || isInequality) && !is_join_clause) {
        s1 = scalararraysel_containment(root, leftop, rightop, nominal_element_type,
                                       isEquality, useOr, varRelid);
        if (s1 >= 0.0) {
            return s1;
        }
    }

    // Get operator's selectivity function
    if (is_join_clause) {
        oprsel = get_oprjoin(operator);
    } else {
        oprsel = get_oprrest(operator);
    }
    if (!oprsel) {
        return (Selectivity) 0.5;
    }
    fmgr_info(oprsel, &oprselproc);

    // Detect equality/inequality by selectivity function
    if (oprsel == F_EQSEL || oprsel == F_EQJOINSEL) {
        isEquality = true;
    } else if (oprsel == F_NEQSEL || oprsel == F_NEQJOINSEL) {
        isInequality = true;
    }

    // Process different array types
    if (rightop && IsA(rightop, Const)) {
        // Constant array - process each element
        Datum arraydatum = ((Const *) rightop)->constvalue;
        bool arrayisnull = ((Const *) rightop)->constisnull;

        if (arrayisnull) {
            return (Selectivity) 0.0;
        }

        ArrayType *arrayval = DatumGetArrayTypeP(arraydatum);
        // ... array deconstruction and element processing logic ...
        // (Simplified for brevity - processes each array element)

    } else if (rightop && IsA(rightop, ArrayExpr)) {
        // ARRAY[] construct - process each element expression
        // ... element processing logic similar to constant array ...

    } else {
        // Unknown array - use dummy element and assume 10 elements
        CaseTestExpr *dummyexpr = makeNode(CaseTestExpr);
        dummyexpr->typeId = nominal_element_type;
        dummyexpr->typeMod = -1;
        dummyexpr->collation = clause->inputcollid;

        List *args = list_make2(leftop, dummyexpr);
        Selectivity s2;

        // Get selectivity for single comparison
        if (is_join_clause) {
            s2 = DatumGetFloat8(FunctionCall5Coll(&oprselproc, clause->inputcollid,
                               PointerGetDatum(root), ObjectIdGetDatum(operator),
                               PointerGetDatum(args), Int16GetDatum(jointype),
                               PointerGetDatum(sjinfo)));
        } else {
            s2 = DatumGetFloat8(FunctionCall4Coll(&oprselproc, clause->inputcollid,
                               PointerGetDatum(root), ObjectIdGetDatum(operator),
                               PointerGetDatum(args), Int32GetDatum(varRelid)));
        }

        // Combine for assumed 10 elements
        s1 = useOr ? 0.0 : 1.0;
        for (int i = 0; i < 10; i++) {
            if (useOr) {
                s1 = s1 + s2 - s1 * s2;
            } else {
                s1 = s1 * s2;
            }
        }
    }

    CLAMP_PROBABILITY(s1);
    return s1;
}
```