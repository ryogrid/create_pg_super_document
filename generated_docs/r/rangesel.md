# rangesel

## Location
[src/backend/utils/adt/rangetypes_selfuncs.c:104-228](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes_selfuncs.c#L104-L228)

## Overview
Calculates restriction selectivity for range operators to estimate how many rows will match a range condition during query planning.

## Definition
```c
Datum rangesel(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL selectivity function that estimates the fraction of rows that will satisfy a range operator condition. It's used by the query planner to make cost-based decisions about query execution plans. The function handles various range operators and uses statistics to provide accurate selectivity estimates.

The function performs several validations and transformations:
1. Validates that the expression is in the form (variable op constant) or (constant op variable)
2. Handles NULL constants by returning 0.0 selectivity
3. Commutes operators when the variable is on the right side
4. Converts element containment operations to range operations
5. Uses statistical analysis via calc_rangesel when possible

## Parameters / Member Variables
Function arguments accessed via PG_GETARG_* macros:
- `root`: PlannerInfo structure containing query planning context
- `operator`: OID of the range operator being evaluated
- `args`: List of arguments to the operator
- `varRelid`: Relation ID for the variable being tested

## Dependencies
- Functions called/Symbols referenced:
  - [get_restriction_variable](../g/get_restriction_variable.md)
  - [default_range_selectivity](../d/default_range_selectivity.md)
  - [get_commutator](../g/get_commutator.md)
  - [range_get_typcache](../r/range_get_typcache.md)
  - [range_serialize](../r/range_serialize.md)
  - [calc_rangesel](../c/calc_rangesel.md)
  - [DatumGetRangeTypeP](../D/DatumGetRangeTypeP.md)
  - ReleaseVariableStats
  - CLAMP_PROBABILITY

## Notes and Other Information
- This is a selectivity estimation function called by the PostgreSQL query planner
- Handles special cases for element containment operators
- Returns default estimates when statistical analysis is not possible
- Part of the range types selectivity estimation infrastructure

## Simplified Source

```c
Datum rangesel(PG_FUNCTION_ARGS) {
    PlannerInfo *root = (PlannerInfo *) PG_GETARG_POINTER(0);
    Oid operator = PG_GETARG_OID(1);
    List *args = (List *) PG_GETARG_POINTER(2);
    int varRelid = PG_GETARG_INT32(3);
    VariableStatData vardata;
    Node *other;
    bool varonleft;
    Selectivity selec;
    TypeCacheEntry *typcache = NULL;
    RangeType *constrange = NULL;

    // Validate expression format (variable op constant)
    if (!get_restriction_variable(root, args, varRelid, &vardata, &other, &varonleft))
        PG_RETURN_FLOAT8(default_range_selectivity(operator));

    // Require constant on one side
    if (!IsA(other, Const)) {
        ReleaseVariableStats(vardata);
        PG_RETURN_FLOAT8(default_range_selectivity(operator));
    }

    // Handle NULL constants
    if (((Const *) other)->constisnull) {
        ReleaseVariableStats(vardata);
        PG_RETURN_FLOAT8(0.0);
    }

    // Commute operator if variable is on right side
    if (!varonleft) {
        operator = get_commutator(operator);
        if (!operator) {
            ReleaseVariableStats(vardata);
            PG_RETURN_FLOAT8(default_range_selectivity(operator));
        }
    }

    // Handle element containment operations
    if (operator == OID_RANGE_CONTAINS_ELEM_OP) {
        typcache = range_get_typcache(fcinfo, vardata.vartype);
        if (((Const *) other)->consttype == typcache->rngelemtype->type_id) {
            // Convert element to single-point range
            RangeBound lower, upper;
            lower.inclusive = true;
            lower.val = ((Const *) other)->constvalue;
            lower.infinite = false;
            lower.lower = true;
            upper.inclusive = true;
            upper.val = ((Const *) other)->constvalue;
            upper.infinite = false;
            upper.lower = false;
            constrange = range_serialize(typcache, &lower, &upper, false, NULL);
        }
    } else if (((Const *) other)->consttype == vardata.vartype) {
        // Both sides are same range type
        typcache = range_get_typcache(fcinfo, vardata.vartype);
        constrange = DatumGetRangeTypeP(((Const *) other)->constvalue);
    }

    // Calculate selectivity using statistics or default
    if (constrange)
        selec = calc_rangesel(typcache, &vardata, constrange, operator);
    else
        selec = default_range_selectivity(operator);

    ReleaseVariableStats(vardata);
    CLAMP_PROBABILITY(selec);
    PG_RETURN_FLOAT8((float8) selec);
}
```