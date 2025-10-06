# networkjoinsel_inner

## Location
[src/backend/utils/adt/network_selfuncs.c:263-389](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_selfuncs.c#L263-L389)

## Overview
Calculates inner join selectivity estimation for network subnet inclusion/overlap operators by evaluating MCV vs MCV, MCV vs histogram, and histogram vs histogram match probabilities.

## Definition
```c
static Selectivity networkjoinsel_inner(Oid operator, VariableStatData *vardata1, VariableStatData *vardata2)
```

## Detailed Description
The `networkjoinsel_inner` function implements sophisticated selectivity estimation for inner joins involving network operators. Unlike equality joins where one-to-one matching suffices, network inclusion operators can match many-to-many relationships, requiring comprehensive pairwise evaluation of all possible matches.

The function systematically computes selectivity across three scenarios:
1. **MCV vs MCV**: Direct comparison of most common values from both relations
2. **MCV vs Histogram**: Matches between common values and histogram-distributed values  
3. **Histogram vs Histogram**: Cross-histogram analysis for comprehensive coverage

Each calculation is properly weighted by the population fractions represented by the respective statistics (accounting for null fractions and MCV coverage). The function limits MCV consideration to MAX_CONSIDERED_ELEMS (1024) for performance, and includes provisions for operator commutation when necessary.

## Parameters / Member Variables
- `operator`: OID of the network operator (e.g., subnet inclusion, overlap)
- `vardata1`: Statistical data for the first join variable
- `vardata2`: Statistical data for the second join variable

## Dependencies
- Functions called/Symbols referenced:
  - [get_attstatsslot](../g/get_attstatsslot.md)
  - [mcv_population](../m/mcv_population.md)  
  - [inet_opr_codenum](../i/inet_opr_codenum.md)
  - [inet_mcv_join_sel](../i/inet_mcv_join_sel.md)
  - [inet_mcv_hist_sel](../i/inet_mcv_hist_sel.md)
  - [inet_hist_inclusion_join_sel](../i/inet_hist_inclusion_join_sel.md)
  - [free_attstatsslot](../f/free_attstatsslot.md)
  - DEFAULT_SEL
- Called from (representative examples):
  - [networkjoinsel](networkjoinsel.md)

## Notes and Other Information
- Handles cases where statistics are unavailable by falling back to default selectivity with null fraction adjustments
- Unlike `eqjoinsel_inner`, does not neglect MCV vs histogram selectivity due to many-to-many nature of network operations
- Implements performance optimization by limiting MCV arrays to 1024 elements maximum
- Properly scales selectivities by population fractions to avoid double-counting
- Uses operator commutation (-opr_codenum) when evaluating second relation's MCV against first relation's histogram
- Comprehensive error handling for missing statistics with appropriate fallback behavior

## Simplified Source

```c
static Selectivity
networkjoinsel_inner(Oid operator, VariableStatData *vardata1, VariableStatData *vardata2)
{
    Form_pg_statistic stats;
    double nullfrac1 = 0.0, nullfrac2 = 0.0;
    Selectivity selec = 0.0, sumcommon1 = 0.0, sumcommon2 = 0.0;
    bool mcv1_exists = false, mcv2_exists = false, hist1_exists = false, hist2_exists = false;
    int opr_codenum, mcv1_length = 0, mcv2_length = 0;
    AttStatsSlot mcv1_slot, mcv2_slot, hist1_slot, hist2_slot;

    // Extract statistics for first variable
    if (HeapTupleIsValid(vardata1->statsTuple))
    {
        stats = (Form_pg_statistic) GETSTRUCT(vardata1->statsTuple);
        nullfrac1 = stats->stanullfrac;

        mcv1_exists = get_attstatsslot(&mcv1_slot, vardata1->statsTuple, STATISTIC_KIND_MCV, InvalidOid, ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS);
        hist1_exists = get_attstatsslot(&hist1_slot, vardata1->statsTuple, STATISTIC_KIND_HISTOGRAM, InvalidOid, ATTSTATSSLOT_VALUES);

        mcv1_length = Min(mcv1_slot.nvalues, MAX_CONSIDERED_ELEMS);
        if (mcv1_exists)
            sumcommon1 = mcv_population(mcv1_slot.numbers, mcv1_length);
    }
    else
    {
        memset(&mcv1_slot, 0, sizeof(mcv1_slot));
        memset(&hist1_slot, 0, sizeof(hist1_slot));
    }

    // Extract statistics for second variable
    if (HeapTupleIsValid(vardata2->statsTuple))
    {
        stats = (Form_pg_statistic) GETSTRUCT(vardata2->statsTuple);
        nullfrac2 = stats->stanullfrac;

        mcv2_exists = get_attstatsslot(&mcv2_slot, vardata2->statsTuple, STATISTIC_KIND_MCV, InvalidOid, ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS);
        hist2_exists = get_attstatsslot(&hist2_slot, vardata2->statsTuple, STATISTIC_KIND_HISTOGRAM, InvalidOid, ATTSTATSSLOT_VALUES);

        mcv2_length = Min(mcv2_slot.nvalues, MAX_CONSIDERED_ELEMS);
        if (mcv2_exists)
            sumcommon2 = mcv_population(mcv2_slot.numbers, mcv2_length);
    }
    else
    {
        memset(&mcv2_slot, 0, sizeof(mcv2_slot));
        memset(&hist2_slot, 0, sizeof(hist2_slot));
    }

    opr_codenum = inet_opr_codenum(operator);

    // Calculate MCV vs MCV selectivity
    if (mcv1_exists && mcv2_exists)
        selec += inet_mcv_join_sel(mcv1_slot.values, mcv1_slot.numbers, mcv1_length, mcv2_slot.values, mcv2_slot.numbers, mcv2_length, operator);

    // Add MCV vs histogram selectivities (both directions)
    if (mcv1_exists && hist2_exists)
        selec += (1.0 - nullfrac2 - sumcommon2) * inet_mcv_hist_sel(mcv1_slot.values, mcv1_slot.numbers, mcv1_length, hist2_slot.values, hist2_slot.nvalues, opr_codenum);

    if (mcv2_exists && hist1_exists)
        selec += (1.0 - nullfrac1 - sumcommon1) * inet_mcv_hist_sel(mcv2_slot.values, mcv2_slot.numbers, mcv2_length, hist1_slot.values, hist1_slot.nvalues, -opr_codenum);

    // Add histogram vs histogram selectivity
    if (hist1_exists && hist2_exists)
        selec += (1.0 - nullfrac1 - sumcommon1) * (1.0 - nullfrac2 - sumcommon2) * inet_hist_inclusion_join_sel(hist1_slot.values, hist1_slot.nvalues, hist2_slot.values, hist2_slot.nvalues, opr_codenum);

    // Fallback to default if no useful statistics
    if ((!mcv1_exists && !hist1_exists) || (!mcv2_exists && !hist2_exists))
        selec = (1.0 - nullfrac1) * (1.0 - nullfrac2) * DEFAULT_SEL(operator);

    // Clean up statistics slots
    free_attstatsslot(&mcv1_slot);
    free_attstatsslot(&mcv2_slot);
    free_attstatsslot(&hist1_slot);
    free_attstatsslot(&hist2_slot);

    return selec;
}
```