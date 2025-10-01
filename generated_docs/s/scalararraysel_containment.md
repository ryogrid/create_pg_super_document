# scalararraysel_containment

## Location
[src/backend/utils/adt/array_selfuncs.c:81-240](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/array_selfuncs.c#L81-L240)

## Overview
Estimates selectivity of ScalarArrayOpExpr operations via array containment analysis, converting expressions like 'const =/<> ANY/ALL (array_var)' into equivalent array containment operations.

## Definition

```c
Selectivity
scalararraysel_containment(PlannerInfo *root,
						   Node *leftop, Node *rightop,
						   Oid elemtype, bool isEquality, bool useOr,
						   int varRelid)
```
## Detailed Description
This function provides selectivity estimation for scalar array operations by transforming them into array containment operations. It handles expressions of the form 'const =/<> ANY/ALL (array_var)' by treating them as array containment operations like 'array_var op ARRAY[const]'.

The function distinguishes between two cases:
- For = ANY operations: estimates as 'var @> ARRAY[const]' (contains)
- For = ALL operations: estimates as 'var <@ ARRAY[const]' (contained by)

For inequality operators (<>), the function swaps ANY/ALL semantics and inverts the final result. The estimation relies on most-common-elements (MCE) statistics and distinct-element count histograms when available.

## Parameters
- : PlannerInfo containing query planning context
- : Left operand node (must be a constant)
- : Right operand node (must be a variable)
- : OID of the array element type
- : true for = operator, false for <> operator
- : true for ANY semantics, false for ALL semantics
- : Variable relation ID for statistics lookup

## Dependencies
- Functions called/Symbols referenced:
  - [examine_variable](../e/examine_variable.md)
  - ReleaseVariableStats
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - [statistic_proc_security_check](statistic_proc_security_check.md)
  - [get_attstatsslot](../g/get_attstatsslot.md)
  - [mcelem_array_contain_overlap_selec](../m/mcelem_array_contain_overlap_selec.md)
  - [mcelem_array_contained_selec](../m/mcelem_array_contained_selec.md)
  - [free_attstatsslot](../f/free_attstatsslot.md)
  - CLAMP_PROBABILITY
- Called from:
  - [scalararraysel](scalararraysel.md) (src/backend/utils/adt/selfuncs.c:1878)

## Notes and Other Information
- Returns selectivity value between 0 and 1, or -1 if estimation fails
- Requires the left operand to be a constant and right operand to be a variable
- Uses array element statistics (MCELEM and DECHIST) when available
- Adjusts for null fraction in the statistics
- Part of PostgreSQL's cost-based optimizer selectivity estimation framework

## Simplified Source

```c
Selectivity
scalararraysel_containment(PlannerInfo *root, Node *leftop, Node *rightop,
                          Oid elemtype, bool isEquality, bool useOr, int varRelid)
{
    Selectivity selec;
    VariableStatData vardata;
    Datum constval;
    TypeCacheEntry *typentry;
    FmgrInfo *cmpfunc;

    // Verify rightop is a variable with statistics
    examine_variable(root, rightop, varRelid, &vardata);
    if (!vardata.rel) {
        ReleaseVariableStats(vardata);
        return -1.0;
    }

    // Verify leftop is a non-null constant
    if (!IsA(leftop, Const) || ((Const *) leftop)->constisnull) {
        ReleaseVariableStats(vardata);
        return (IsA(leftop, Const)) ? 0.0 : -1.0;
    }
    constval = ((Const *) leftop)->constvalue;

    // Get element type's comparison function
    typentry = lookup_type_cache(elemtype, TYPECACHE_CMP_PROC_FINFO);
    if (!OidIsValid(typentry->cmp_proc_finfo.fn_oid)) {
        ReleaseVariableStats(vardata);
        return -1.0;
    }
    cmpfunc = &typentry->cmp_proc_finfo;

    // For <> operator, swap ANY/ALL semantics
    if (!isEquality)
        useOr = !useOr;

    // Use array element statistics if available
    if (HeapTupleIsValid(vardata.statsTuple) &&
        statistic_proc_security_check(&vardata, cmpfunc->fn_oid)) {

        AttStatsSlot sslot, hslot;

        // Get most-common-elements statistics
        if (get_attstatsslot(&sslot, vardata.statsTuple, STATISTIC_KIND_MCELEM,
                            InvalidOid, ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS)) {

            // For ALL case, also get distinct-element count histogram
            if (!useOr) {
                get_attstatsslot(&hslot, vardata.statsTuple, STATISTIC_KIND_DECHIST,
                               InvalidOid, ATTSTATSSLOT_NUMBERS);
            }

            // Estimate selectivity based on operation type
            if (useOr) {
                // = ANY: estimate as array @> ARRAY[const]
                selec = mcelem_array_contain_overlap_selec(sslot.values, sslot.nvalues,
                                                         sslot.numbers, sslot.nnumbers,
                                                         &constval, 1,
                                                         OID_ARRAY_CONTAINS_OP, typentry);
            } else {
                // = ALL: estimate as array <@ ARRAY[const]
                selec = mcelem_array_contained_selec(sslot.values, sslot.nvalues,
                                                   sslot.numbers, sslot.nnumbers,
                                                   &constval, 1, hslot.numbers, hslot.nnumbers,
                                                   OID_ARRAY_CONTAINED_OP, typentry);
            }

            // Adjust for null fraction
            Form_pg_statistic stats = (Form_pg_statistic) GETSTRUCT(vardata.statsTuple);
            selec *= (1.0 - stats->stanullfrac);

            free_attstatsslot(&sslot);
            if (!useOr) free_attstatsslot(&hslot);
        } else {
            // No MCE statistics - use fallback estimation
            selec = (useOr) ?
                mcelem_array_contain_overlap_selec(NULL, 0, NULL, 0, &constval, 1, OID_ARRAY_CONTAINS_OP, typentry) :
                mcelem_array_contained_selec(NULL, 0, NULL, 0, &constval, 1, NULL, 0, OID_ARRAY_CONTAINED_OP, typentry);
        }
    } else {
        // No statistics - use default estimation
        selec = (useOr) ?
            mcelem_array_contain_overlap_selec(NULL, 0, NULL, 0, &constval, 1, OID_ARRAY_CONTAINS_OP, typentry) :
            mcelem_array_contained_selec(NULL, 0, NULL, 0, &constval, 1, NULL, 0, OID_ARRAY_CONTAINED_OP, typentry);
    }

    ReleaseVariableStats(vardata);

    // For <> operator, invert the result
    if (!isEquality)
        selec = 1.0 - selec;

    CLAMP_PROBABILITY(selec);
    return selec;
}
```