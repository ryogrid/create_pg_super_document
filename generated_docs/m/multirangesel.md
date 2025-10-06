# multirangesel

## Location
[src/backend/utils/adt/multirangetypes_selfuncs.c:137-290](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes_selfuncs.c#L137-L290)

## Overview
PostgreSQL selectivity estimation function for multirange operators that calculates the probability of rows matching multirange query conditions.

## Definition
```c
Datum multirangesel(PG_FUNCTION_ARGS)
```

## Detailed Description
This is the main entry point for multirange operator selectivity estimation in the PostgreSQL query planner. The function analyzes query conditions involving multirange operators and estimates what fraction of rows will match the condition.

The function performs several key steps:
1. Extracts the operator, arguments, and variable information from the planner context
2. Validates that the expression is in the form "variable op constant" or "constant op variable"
3. Handles NULL constants (returns 0.0 selectivity)
4. Commutes operators when the variable is on the right side
5. Handles special cases like element containment (@>) by converting elements to singleton multiranges
6. Promotes range constants to multiranges for mixed-type operations
7. Delegates to calc_multirangesel() for detailed statistical analysis or falls back to default estimates

## Parameters / Member Variables
- `root`: PlannerInfo pointer containing query planning context
- `operator`: OID of the multirange operator being estimated
- `args`: List of arguments to the operator (typically variable and constant)
- `varRelid`: Relation ID for the variable, or 0 if multiple relations

## Dependencies
- Functions called/Symbols referenced:
  - [get_restriction_variable](../g/get_restriction_variable.md)
  - [default_multirange_selectivity](../d/default_multirange_selectivity.md)
  - [get_commutator](../g/get_commutator.md)
  - [multirange_get_typcache](multirange_get_typcache.md)
  - [range_serialize](../r/range_serialize.md)
  - [make_multirange](make_multirange.md)
  - [DatumGetRangeTypeP](../D/DatumGetRangeTypeP.md)
  - [DatumGetMultirangeTypeP](../D/DatumGetMultirangeTypeP.md)
  - [calc_multirangesel](../c/calc_multirangesel.md)
  - ReleaseVariableStats
  - CLAMP_PROBABILITY

- Called from (representative examples):
  - PostgreSQL query planner during selectivity estimation phase

## Notes and Other Information
This function is registered in PostgreSQL's system catalogs as the selectivity estimator for multirange operators. It handles complex type conversions between elements, ranges, and multiranges to provide unified selectivity estimation. The function ensures all estimates are clamped to valid probability values [0.0, 1.0] before returning.

## Simplified Source

```c
Datum
multirangesel(PG_FUNCTION_ARGS)
{
    PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
    Oid operator = PG_GETARG_OID(1);
    List *args = (List *) PG_GETARG_POINTER(2);
    int varRelid = PG_GETARG_INT32(3);
    VariableStatData vardata;
    Node *other;
    bool varonleft;
    Selectivity selec;
    TypeCacheEntry *typcache = NULL;
    MultirangeType *constmultirange = NULL;
    RangeType *constrange = NULL;

    // Must be in form "variable op constant" or "constant op variable"
    if (!get_restriction_variable(root, args, varRelid, &vardata, &other, &varonleft))
        PG_RETURN_FLOAT8(default_multirange_selectivity(operator));

    // Constant required for meaningful selectivity estimation
    if (!IsA(other, Const)) {
        ReleaseVariableStats(vardata);
        PG_RETURN_FLOAT8(default_multirange_selectivity(operator));
    }

    // NULL constants always result in no matches
    if (((Const *) other)->constisnull) {
        ReleaseVariableStats(vardata);
        PG_RETURN_FLOAT8(0.0);
    }

    // Commute operator if variable is on right side
    if (!varonleft) {
        operator = get_commutator(operator);
        if (!operator) {
            ReleaseVariableStats(vardata);
            PG_RETURN_FLOAT8(default_multirange_selectivity(operator));
        }
    }

    // Handle special case: multirange @> element
    if (operator == OID_MULTIRANGE_CONTAINS_ELEM_OP) {
        typcache = multirange_get_typcache(fcinfo, vardata.vartype);
        if (((Const *) other)->consttype == typcache->rngtype->rngelemtype->type_id) {
            // Convert element to singleton range, then to multirange
            RangeBound lower, upper;
            lower.inclusive = upper.inclusive = true;
            lower.val = upper.val = ((Const *) other)->constvalue;
            lower.infinite = upper.infinite = false;
            lower.lower = true; upper.lower = false;

            constrange = range_serialize(typcache->rngtype, &lower, &upper, false, NULL);
            constmultirange = make_multirange(typcache->type_id, typcache->rngtype, 1, &constrange);
        }
    }
    // Handle multirange-range operations
    else if (operator == OID_RANGE_MULTIRANGE_CONTAINED_OP ||
             operator == OID_MULTIRANGE_CONTAINS_RANGE_OP ||
             /* ... other range operators ... */) {
        typcache = multirange_get_typcache(fcinfo, vardata.vartype);
        if (((Const *) other)->consttype == typcache->rngtype->type_id) {
            // Convert range to multirange
            constrange = DatumGetRangeTypeP(((Const *) other)->constvalue);
            constmultirange = make_multirange(typcache->type_id, typcache->rngtype, 1, &constrange);
        }
    }
    // Handle multirange-multirange operations
    else if (((Const *) other)->consttype == vardata.vartype) {
        typcache = multirange_get_typcache(fcinfo, vardata.vartype);
        constmultirange = DatumGetMultirangeTypeP(((Const *) other)->constvalue);
    }

    // Calculate selectivity or use default
    if (constmultirange)
        selec = calc_multirangesel(typcache, &vardata, constmultirange, operator);
    else
        selec = default_multirange_selectivity(operator);

    ReleaseVariableStats(vardata);
    CLAMP_PROBABILITY(selec);
    PG_RETURN_FLOAT8((float8) selec);
}
```