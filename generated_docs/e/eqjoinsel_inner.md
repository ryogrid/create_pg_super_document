# eqjoinsel_inner

## Location
[src/backend/utils/adt/selfuncs.c:2438-2634](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/selfuncs.c#L2438-L2634)

## Overview
Computes join selectivity for normal inner joins (and LEFT/FULL outer joins) using detailed statistical analysis, particularly leveraging Most Common Values (MCVs) when available for highly accurate estimates.

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
This function implements the core logic for estimating equality join selectivity using two distinct approaches:

**MCV-based estimation (when both sides have MCVs):**
- Performs actual equality comparisons between MCV lists from both relations
- Calculates exact selectivity for the portion represented by MCVs
- Estimates selectivity for non-MCV populations using statistical extrapolation
- Uses the mathematical framework from Ioannidis and Christodoulakis research on join size estimation accuracy

**Statistical estimation (fallback approach):**
- Uses the formula MIN(1/nd1, 1/nd2) * (1-nullfrac1) * (1-nullfrac2)
- Assumes equal distribution of non-null values
- Takes the minimum to estimate from the perspective of the relation with smaller distinct value count

The MCV approach provides significantly higher accuracy for skewed distributions by computing exact matches for the most frequent values, then extrapolating to estimate the remaining population.

## Parameters / Member Variables
- : OID of the equality comparison function
- : Collation to use for string comparisons
- , : Variable statistical data for both join sides
- , : Number of distinct values for each variable
- , : Whether the distinct value counts are defaults
- , : Attribute statistics slots containing MCV data
- , : PostgreSQL statistics forms with null fractions
- , : Whether MCV statistics are available for each side

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
- The function assumes each MCV matches at most one member of the other MCV list for performance and mathematical consistency
- Memory is carefully managed with palloc0 for temporary arrays and pfree for cleanup
- Probability values are clamped to ensure they remain within valid [0,1] bounds
- The algorithm handles null fractions explicitly in both estimation approaches
- For relations without MCV statistics, the function falls back to a conservative but mathematically sound estimate
- The choice of smaller estimate between totalsel1 and totalsel2 provides a more reliable bound by considering the limiting relation
- This function is also used for LEFT and FULL outer joins since the additional complexity of distinguishing them isn't currently deemed worthwhile

## Simplified Source

```c
static double
eqjoinsel_inner(Oid opfuncoid, Oid collation,
                VariableStatData *vardata1, VariableStatData *vardata2,
                double nd1, double nd2,
                bool isdefault1, bool isdefault2,
                AttStatsSlot *sslot1, AttStatsSlot *sslot2,
                Form_pg_statistic stats1, Form_pg_statistic stats2,
                bool have_mcvs1, bool have_mcvs2)
{
    double selec;

    if (have_mcvs1 && have_mcvs2) {
        /*
         * We have MCV lists for both sides - perform detailed analysis by
         * comparing MCVs and calculating exact selectivity for the MCV portion,
         * then estimating the remainder.
         */
        LOCAL_FCINFO(fcinfo, 2);
        FmgrInfo eqproc;
        bool *hasmatch1, *hasmatch2;
        double nullfrac1 = stats1->stanullfrac;
        double nullfrac2 = stats2->stanullfrac;
        double matchprodfreq, matchfreq1, matchfreq2;
        double unmatchfreq1, unmatchfreq2, otherfreq1, otherfreq2;
        double totalsel1, totalsel2;
        int i, nmatches;

        // Set up function call for equality comparisons
        fmgr_info(opfuncoid, &eqproc);
        InitFunctionCallInfoData(*fcinfo, &eqproc, 2, collation, NULL, NULL);
        fcinfo->args[0].isnull = false;
        fcinfo->args[1].isnull = false;

        // Track which MCVs have matches
        hasmatch1 = (bool *) palloc0(sslot1->nvalues * sizeof(bool));
        hasmatch2 = (bool *) palloc0(sslot2->nvalues * sizeof(bool));

        // Compare all MCV pairs to find matches
        matchprodfreq = 0.0;
        nmatches = 0;
        for (i = 0; i < sslot1->nvalues; i++) {
            fcinfo->args[0].value = sslot1->values[i];

            for (int j = 0; j < sslot2->nvalues; j++) {
                if (hasmatch2[j])
                    continue;

                fcinfo->args[1].value = sslot2->values[j];
                fcinfo->isnull = false;
                Datum fresult = FunctionCallInvoke(fcinfo);

                if (!fcinfo->isnull && DatumGetBool(fresult)) {
                    hasmatch1[i] = hasmatch2[j] = true;
                    matchprodfreq += sslot1->numbers[i] * sslot2->numbers[j];
                    nmatches++;
                    break;
                }
            }
        }
        CLAMP_PROBABILITY(matchprodfreq);

        // Calculate frequencies for matched and unmatched MCVs
        matchfreq1 = unmatchfreq1 = 0.0;
        for (i = 0; i < sslot1->nvalues; i++) {
            if (hasmatch1[i])
                matchfreq1 += sslot1->numbers[i];
            else
                unmatchfreq1 += sslot1->numbers[i];
        }

        matchfreq2 = unmatchfreq2 = 0.0;
        for (i = 0; i < sslot2->nvalues; i++) {
            if (hasmatch2[i])
                matchfreq2 += sslot2->numbers[i];
            else
                unmatchfreq2 += sslot2->numbers[i];
        }

        // Calculate frequency of non-MCV values
        otherfreq1 = 1.0 - nullfrac1 - matchfreq1 - unmatchfreq1;
        otherfreq2 = 1.0 - nullfrac2 - matchfreq2 - unmatchfreq2;
        CLAMP_PROBABILITY(otherfreq1);
        CLAMP_PROBABILITY(otherfreq2);

        // Estimate total selectivity from both perspectives
        totalsel1 = matchprodfreq;
        if (nd2 > sslot2->nvalues)
            totalsel1 += unmatchfreq1 * otherfreq2 / (nd2 - sslot2->nvalues);
        if (nd2 > nmatches)
            totalsel1 += otherfreq1 * (otherfreq2 + unmatchfreq2) / (nd2 - nmatches);

        totalsel2 = matchprodfreq;
        if (nd1 > sslot1->nvalues)
            totalsel2 += unmatchfreq2 * otherfreq1 / (nd1 - sslot1->nvalues);
        if (nd1 > nmatches)
            totalsel2 += otherfreq2 * (otherfreq1 + unmatchfreq1) / (nd1 - nmatches);

        // Use the more conservative estimate
        selec = (totalsel1 < totalsel2) ? totalsel1 : totalsel2;

        pfree(hasmatch1);
        pfree(hasmatch2);
    } else {
        /*
         * No MCV data available - use statistical approximation
         * Formula: MIN(1/nd1, 1/nd2) * (1-nullfrac1) * (1-nullfrac2)
         */
        double nullfrac1 = stats1 ? stats1->stanullfrac : 0.0;
        double nullfrac2 = stats2 ? stats2->stanullfrac : 0.0;

        selec = (1.0 - nullfrac1) * (1.0 - nullfrac2);
        if (nd1 > nd2)
            selec /= nd1;
        else
            selec /= nd2;
    }

    return selec;
}
```