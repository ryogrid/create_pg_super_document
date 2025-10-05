# patternsel_common

## Location
[src/backend/utils/adt/like_support.c:486-759](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/like_support.c#L486-L759)

## Overview
A comprehensive selectivity estimation function for pattern matching operations that combines histogram analysis, most-common-values statistics, and heuristic methods to predict how many rows will match a given pattern.

## Definition
```c
static double patternsel_common(PlannerInfo *root, Oid oprid, Oid opfuncid, List *args, int varRelid, Oid collation, Pattern_Type ptype, bool negate)
```

## Detailed Description
The `patternsel_common` function is PostgreSQL's sophisticated selectivity estimator for pattern matching operations like LIKE, ILIKE, and regular expressions. It provides the query planner with accurate estimates of how many rows will match a pattern, which is crucial for choosing optimal query execution plans.

The function employs a multi-layered approach:

1. **Pattern Analysis**: Extracts fixed prefixes from patterns using `pattern_fixed_prefix` to identify portions that can be estimated more accurately
2. **Exact Match Handling**: For patterns that specify exact values, uses standard equality selectivity estimation via `var_eq_const`
3. **Statistical Analysis**: For non-exact patterns, combines multiple estimation methods:
   - **Histogram Method**: Applies the pattern to histogram entries when sufficient data exists (≥100 entries)
   - **Heuristic Method**: Uses prefix selectivity combined with remainder pattern selectivity for smaller datasets
   - **Hybrid Approach**: Blends histogram and heuristic methods for medium-sized datasets (10-100 entries)
4. **MCV Integration**: Separately analyzes most-common-values by directly applying the pattern operator
5. **Result Composition**: Combines histogram, heuristic, and MCV results while accounting for null values

This sophisticated approach enables PostgreSQL to make informed decisions about index usage, join order, and other execution strategies.

## Parameters / Member Variables
- `root`: Planner information context containing query and table statistics
- `oprid`: OID of the comparison operator (for positive match even when negated)
- `opfuncid`: OID of the underlying function (can be computed from `oprid` if needed)
- `args`: List of arguments to the pattern operation
- `varRelid`: Relation ID for variable statistics lookup
- `collation`: Collation OID to use for pattern matching
- `ptype`: Type of pattern operation (`Pattern_Type` enum: LIKE, ILIKE, regex, etc.)
- `negate`: Whether to estimate selectivity for the negated operation (NOT LIKE, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - `[get_restriction_variable](../g/get_restriction_variable.md)`: Extracts variable and constant from operation arguments
  - [pattern_fixed_prefix](pattern_fixed_prefix.md): Extracts fixed prefix and estimates remainder selectivity
  - [var_eq_const](../v/var_eq_const.md): Estimates selectivity for exact equality comparisons
  - `[histogram_selectivity](../h/histogram_selectivity.md)`: Applies pattern to histogram entries for selectivity estimation
  - [prefix_selectivity](prefix_selectivity.md): Estimates selectivity for prefix-based range operations
  - `[mcv_selectivity](../m/mcv_selectivity.md)`: Analyzes most-common-values against the pattern
  - `ReleaseVariableStats`: Frees variable statistics resources
  - `CLAMP_PROBABILITY`: Ensures result stays within valid probability range
- Called from (representative examples):
  - [like_regex_support](../l/like_regex_support.md): Pattern support dispatcher for selectivity requests
  - [patternsel](patternsel.md): Main entry point for pattern selectivity estimation

## Notes and Other Information
- Supports TEXT, NAME, BPCHAR (char), and BYTEA data types
- Returns default estimate (`DEFAULT_MATCH_SEL`) when unable to analyze the pattern or statistics
- Handles null constants by returning 0.0 selectivity (strict operators never match nulls)
- Uses actual operator collation for pattern analysis to ensure runtime cache compatibility
- Automatically coerces text constants to bpchar when needed for type compatibility
- Applies confidence bounds (0.0001 to 0.9999) to prevent extreme selectivity estimates
- Properly accounts for null fraction when computing final selectivity
- For negated operations, computes `1.0 - positive_selectivity - null_fraction`
- Memory management includes cleanup of dynamically allocated prefix constants

## Simplified Source

```c
static double patternsel_common(PlannerInfo *root, Oid oprid, Oid opfuncid,
                               List *args, int varRelid, Oid collation,
                               Pattern_Type ptype, bool negate) {
    VariableStatData vardata;
    Node *other;
    bool varonleft;
    Datum constval;
    Oid consttype, vartype, rdatatype;
    Oid eqopr, ltopr, geopr;
    Pattern_Prefix_Status pstatus;
    Const *patt, *prefix = NULL;
    Selectivity rest_selec = 0;
    double nullfrac = 0.0;
    double result;

    // Initialize with default estimate based on negation
    result = negate ? (1.0 - DEFAULT_MATCH_SEL) : DEFAULT_MATCH_SEL;

    // Extract variable and constant from arguments
    if (!get_restriction_variable(root, args, varRelid, &vardata, &other, &varonleft))
        return result;
    if (!varonleft || !IsA(other, Const)) {
        ReleaseVariableStats(vardata);
        return result;
    }

    // Handle null constants
    if (((Const *) other)->constisnull) {
        ReleaseVariableStats(vardata);
        return 0.0;  // Strict operators never match nulls
    }

    constval = ((Const *) other)->constvalue;
    consttype = ((Const *) other)->consttype;

    // Only support text and bytea pattern constants
    if (consttype != TEXTOID && consttype != BYTEAOID) {
        ReleaseVariableStats(vardata);
        return result;
    }

    // Select operators based on variable data type
    vartype = vardata.vartype;
    switch (vartype) {
        case TEXTOID:
            eqopr = TextEqualOperator;
            ltopr = TextLessOperator;
            geopr = TextGreaterEqualOperator;
            rdatatype = TEXTOID;
            break;
        case NAMEOID:
            eqopr = NameEqualTextOperator;
            ltopr = NameLessTextOperator;
            geopr = NameGreaterEqualTextOperator;
            rdatatype = TEXTOID;
            break;
        case BPCHAROID:
            eqopr = BpcharEqualOperator;
            ltopr = BpcharLessOperator;
            geopr = BpcharGreaterEqualOperator;
            rdatatype = BPCHAROID;
            break;
        case BYTEAOID:
            eqopr = ByteaEqualOperator;
            ltopr = ByteaLessOperator;
            geopr = ByteaGreaterEqualOperator;
            rdatatype = BYTEAOID;
            break;
        default:
            ReleaseVariableStats(vardata);
            return result;
    }

    // Extract null fraction from statistics
    if (HeapTupleIsValid(vardata.statsTuple)) {
        Form_pg_statistic stats = (Form_pg_statistic) GETSTRUCT(vardata.statsTuple);
        nullfrac = stats->stanullfrac;
    }

    // Analyze pattern to extract fixed prefix
    patt = (Const *) other;
    pstatus = pattern_fixed_prefix(patt, ptype, collation, &prefix, &rest_selec);

    // Coerce prefix type if needed (text to bpchar)
    if (prefix && prefix->consttype != rdatatype) {
        prefix->consttype = rdatatype;
    }

    if (pstatus == Pattern_Prefix_Exact) {
        // Exact match - use equality selectivity
        result = var_eq_const(&vardata, eqopr, collation, prefix->constvalue,
                             false, true, false);
    } else {
        // Non-exact pattern - combine histogram and heuristic methods
        Selectivity selec;
        int hist_size;
        FmgrInfo opproc;
        double mcv_selec, sumcommon;

        // Get function info for pattern matching
        if (!OidIsValid(opfuncid))
            opfuncid = get_opcode(oprid);
        fmgr_info(opfuncid, &opproc);

        // Try histogram-based estimation
        selec = histogram_selectivity(&vardata, &opproc, collation,
                                    constval, true, 10, 1, &hist_size);

        // For small histograms, combine with heuristic method
        if (hist_size < 100) {
            Selectivity heursel, prefixsel;

            if (pstatus == Pattern_Prefix_Partial)
                prefixsel = prefix_selectivity(root, &vardata, eqopr, ltopr, geopr,
                                             collation, prefix);
            else
                prefixsel = 1.0;
            heursel = prefixsel * rest_selec;

            if (selec < 0)  // Too few histogram entries
                selec = heursel;
            else {
                // Blend histogram and heuristic for medium-sized datasets
                double hist_weight = hist_size / 100.0;
                selec = selec * hist_weight + heursel * (1.0 - hist_weight);
            }
        }

        // Apply confidence bounds
        if (selec < 0.0001)
            selec = 0.0001;
        else if (selec > 0.9999)
            selec = 0.9999;

        // Add most-common-values contribution
        mcv_selec = mcv_selectivity(&vardata, &opproc, collation,
                                   constval, true, &sumcommon);

        // Combine histogram and MCV results
        selec *= 1.0 - nullfrac - sumcommon;
        selec += mcv_selec;
        result = selec;
    }

    // Adjust for negation
    if (negate)
        result = 1.0 - result - nullfrac;

    // Ensure result is within valid probability range
    CLAMP_PROBABILITY(result);

    // Cleanup
    if (prefix) {
        pfree(DatumGetPointer(prefix->constvalue));
        pfree(prefix);
    }
    ReleaseVariableStats(vardata);

    return result;
}
```