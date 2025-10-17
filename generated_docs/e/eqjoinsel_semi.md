# eqjoinsel_semi

## Location
[src/backend/utils/adt/selfuncs.c:2635-2822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L2635-L2822)

## Overview
Computes join selectivity for semi and anti joins, implementing specialized logic that differs significantly from inner joins by estimating the fraction of outer relation rows that have at least one matching partner in the inner relation.

## Definition

```c
struct just once. Using
		 * FunctionCallInvoke directly also avoids failure if the eqproc
		 * returns NULL, though really equality functions should never do
		 * that.
		 */
		InitFunctionCallInfoData(*fcinfo, &eqproc, 2, collation,
								 NULL, NULL);
```
## Detailed Description
This function estimates selectivity for semi and anti joins, which fundamentally ask "which outer relation rows have at least one match in the inner relation?" This differs from inner joins that count all matching pairs.

**Key distinguishing features:**
1. **Asymmetric clamping**: Only nd2 (inner relation distinct values) is clamped to the inner relation size, not nd1. This prevents double-counting selectivity of outer relation restrictions.
2. **Match-based approach**: When MCVs are available, it calculates exact matches for the MCV portion, then estimates the uncertain remainder.
3. **Heuristic estimation**: For non-MCV populations, it uses the heuristic that if nd1 ≤ nd2, most outer rows likely have matches; otherwise, the fraction nd2/nd1 have matches.

**Estimation strategies:**
- **With MCVs**: Computes exact matches for MCV portions, then applies heuristics to estimate uncertain rows
- **Without MCVs**: Falls back to pure heuristic based on distinct value counts
- **Clamping logic**: Ensures nd2 doesn't exceed inner relation size, preventing impossible estimates

The function handles the case where opfuncoid might be InvalidOid (unlike eqjoinsel_inner), making it more robust for complex query scenarios.

## Parameters / Member Variables
- : OID of the equality comparison function (may be InvalidOid)
- : Collation to use for string comparisons
- : Variable statistical data for LHS (outer relation)
- : Variable statistical data for RHS (inner relation)
- , : Number of distinct values for each variable
- , : Whether the distinct value counts are defaults
- , : Attribute statistics slots containing MCV data
- , : PostgreSQL statistics forms with null fractions
- , : Whether MCV statistics are available for each side
- : RelOptInfo for the inner relation (used for size clamping)

## Dependencies
- Functions called/Symbols referenced:
  - [fmgr_info](../f/fmgr_info.md)
  - InitFunctionCallInfoData
  - FunctionCallInvoke
  - CLAMP_PROBABILITY
  - LOCAL_FCINFO
- Called from (representative examples):
  - [eqjoinsel](eqjoinsel.md)

## Notes and Other Information
- Used for both SEMI and ANTI joins since they're estimated the same way
- The asymmetric treatment of nd1 vs nd2 is crucial for correct selectivity estimation in the broader query planning context
- When reliable ndistinct values aren't available, the function conservatively assumes 50% of uncertain rows have join partners
- The clamping mechanism accounts for cases where the inner relation is very small or empty
- Unlike inner join estimation, this focuses on existence of matches rather than counting all match combinations
- The function carefully manages memory allocation for temporary match tracking arrays
- Results represent the fraction of outer relation rows that will survive the semi/anti join condition

## Simplified Source

```c
static double
eqjoinsel_semi(Oid opfuncoid, Oid collation,
               VariableStatData *vardata1, VariableStatData *vardata2,
               double nd1, double nd2,
               bool isdefault1, bool isdefault2,
               AttStatsSlot *sslot1, AttStatsSlot *sslot2,
               Form_pg_statistic stats1, Form_pg_statistic stats2,
               bool have_mcvs1, bool have_mcvs2,
               RelOptInfo *inner_rel)
{
    double selec;

    /*
     * Clamp nd2 to inner relation size - prevents overestimating available
     * distinct values. This asymmetric treatment (only clamping nd2) ensures
     * we don't double-count outer relation restrictions.
     */
    if (vardata2->rel && nd2 >= vardata2->rel->rows) {
        nd2 = vardata2->rel->rows;
        isdefault2 = false;
    }
    if (nd2 >= inner_rel->rows) {
        nd2 = inner_rel->rows;
        isdefault2 = false;
    }

    if (have_mcvs1 && have_mcvs2 && OidIsValid(opfuncoid)) {
        /*
         * We have MCV lists for both sides - analyze matches between MCVs
         * and estimate selectivity for the uncertain remainder.
         */
        LOCAL_FCINFO(fcinfo, 2);
        FmgrInfo eqproc;
        bool *hasmatch1, *hasmatch2;
        double nullfrac1 = stats1->stanullfrac;
        double matchfreq1, uncertainfrac, uncertain;
        int i, nmatches, clamped_nvalues2;

        // Account for nd2 clamping in MCV comparison
        clamped_nvalues2 = Min(sslot2->nvalues, nd2);

        // Set up equality function for MCV comparisons
        fmgr_info(opfuncoid, &eqproc);
        InitFunctionCallInfoData(*fcinfo, &eqproc, 2, collation, NULL, NULL);
        fcinfo->args[0].isnull = false;
        fcinfo->args[1].isnull = false;

        // Track matches between MCV lists
        hasmatch1 = (bool *) palloc0(sslot1->nvalues * sizeof(bool));
        hasmatch2 = (bool *) palloc0(clamped_nvalues2 * sizeof(bool));

        // Find matches between MCVs (each MCV matches at most one other)
        nmatches = 0;
        for (i = 0; i < sslot1->nvalues; i++) {
            fcinfo->args[0].value = sslot1->values[i];

            for (int j = 0; j < clamped_nvalues2; j++) {
                if (hasmatch2[j])
                    continue;

                fcinfo->args[1].value = sslot2->values[j];
                fcinfo->isnull = false;
                Datum fresult = FunctionCallInvoke(fcinfo);

                if (!fcinfo->isnull && DatumGetBool(fresult)) {
                    hasmatch1[i] = hasmatch2[j] = true;
                    nmatches++;
                    break;
                }
            }
        }

        // Calculate frequency of matched MCVs in outer relation
        matchfreq1 = 0.0;
        for (i = 0; i < sslot1->nvalues; i++) {
            if (hasmatch1[i])
                matchfreq1 += sslot1->numbers[i];
        }
        CLAMP_PROBABILITY(matchfreq1);

        pfree(hasmatch1);
        pfree(hasmatch2);

        /*
         * Estimate fraction of non-MCV outer rows that have join partners.
         * If nd1 <= nd2, assume all have partners; otherwise assume nd2/nd1 have partners.
         * If ndistinct estimates are unreliable, conservatively use 50%.
         */
        if (!isdefault1 && !isdefault2) {
            nd1 -= nmatches;
            nd2 -= nmatches;
            if (nd1 <= nd2 || nd2 < 0)
                uncertainfrac = 1.0;
            else
                uncertainfrac = nd2 / nd1;
        } else {
            uncertainfrac = 0.5;
        }

        // Apply uncertainty factor to non-matched, non-null portion
        uncertain = 1.0 - matchfreq1 - nullfrac1;
        CLAMP_PROBABILITY(uncertain);
        selec = matchfreq1 + uncertainfrac * uncertain;

    } else {
        /*
         * No MCV data available - use heuristic based on distinct value counts.
         * If nd1 <= nd2, most outer rows likely have matches.
         * Otherwise, estimate fraction nd2/nd1 have matches.
         */
        double nullfrac1 = stats1 ? stats1->stanullfrac : 0.0;

        if (!isdefault1 && !isdefault2) {
            if (nd1 <= nd2 || nd2 < 0)
                selec = 1.0 - nullfrac1;
            else
                selec = (nd2 / nd1) * (1.0 - nullfrac1);
        } else {
            selec = 0.5 * (1.0 - nullfrac1);
        }
    }

    return selec;
}
```