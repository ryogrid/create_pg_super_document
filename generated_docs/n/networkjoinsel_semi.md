# networkjoinsel_semi

## Location
[src/backend/utils/adt/network_selfuncs.c:390-538](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_selfuncs.c#L390-L538)

## Overview
Calculates semi/anti join selectivity estimation for network subnet inclusion/overlap operators by evaluating match probabilities between left-hand side values and right-hand side statistics.

## Definition
```c
static Selectivity networkjoinsel_semi(Oid operator, VariableStatData *vardata1, VariableStatData *vardata2)
```

## Detailed Description
The `networkjoinsel_semi` function implements selectivity estimation specifically for semi and anti joins involving network operators. Semi joins only care whether at least one matching row exists on the right side, making the calculation fundamentally different from inner joins.

The function systematically processes left-hand side (LHS) values against all available right-hand side (RHS) statistics:

1. **LHS MCV vs RHS Statistics**: Each MCV element is tested against both RHS MCVs and histogram, scaled by the MCV frequency
2. **LHS Histogram vs RHS Statistics**: Histogram elements (excluding first/last outliers) are sampled and tested, with results scaled by population fractions

The algorithm implements decimation for large histograms to maintain performance, calculating selectivity for a representative sample and extrapolating. For histogram processing, it estimates the number of RHS rows represented by the histogram to inform semi join probability calculations.

## Parameters / Member Variables  
- `operator`: OID of the network operator being evaluated
- `vardata1`: Statistical data for the left-hand side (outer) relation
- `vardata2`: Statistical data for the right-hand side (inner) relation

## Dependencies
- Functions called/Symbols referenced:
  - [get_attstatsslot](../g/get_attstatsslot.md)
  - [mcv_population](../m/mcv_population.md)
  - [inet_opr_codenum](../i/inet_opr_codenum.md)  
  - [fmgr_info](../f/fmgr_info.md)
  - [get_opcode](../g/get_opcode.md)
  - [inet_semi_join_sel](../i/inet_semi_join_sel.md)
  - [free_attstatsslot](../f/free_attstatsslot.md)
  - DEFAULT_SEL
- Called from (representative examples):
  - [networkjoinsel](networkjoinsel.md)

## Notes and Other Information
- Excludes first and last histogram elements as outliers, focusing on representative middle values
- Implements histogram decimation with step size k to limit processing to MAX_CONSIDERED_ELEMS
- Calculates RHS histogram weight based on relation row count for accurate semi join probability estimation  
- Each LHS histogram element assumes equal representation of its bucket population
- Falls back to default selectivity with null fraction adjustment when statistics are unavailable
- Properly handles cases where only one side has statistics available
- Performance-optimized for large statistics targets through systematic sampling strategies

## Simplified Source

```c
static Selectivity
networkjoinsel_semi(Oid operator, VariableStatData *vardata1, VariableStatData *vardata2)
{
    Form_pg_statistic stats;
    Selectivity selec = 0.0, sumcommon1 = 0.0, sumcommon2 = 0.0;
    double nullfrac1 = 0.0, nullfrac2 = 0.0, hist2_weight = 0.0;
    bool mcv1_exists = false, mcv2_exists = false, hist1_exists = false, hist2_exists = false;
    int opr_codenum, mcv1_length = 0, mcv2_length = 0;
    FmgrInfo proc;
    AttStatsSlot mcv1_slot, mcv2_slot, hist1_slot, hist2_slot;

    // Extract statistics for LHS (outer) variable
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

    // Extract statistics for RHS (inner) variable
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
    fmgr_info(get_opcode(operator), &proc);

    // Calculate RHS histogram weight for semi join estimation
    if (hist2_exists && vardata2->rel)
        hist2_weight = (1.0 - nullfrac2 - sumcommon2) * vardata2->rel->rows;

    // Process each LHS MCV against RHS statistics
    if (mcv1_exists && (mcv2_exists || hist2_exists))
    {
        for (int i = 0; i < mcv1_length; i++)
        {
            selec += mcv1_slot.numbers[i] * inet_semi_join_sel(mcv1_slot.values[i], mcv2_exists, mcv2_slot.values, mcv2_length, hist2_exists, hist2_slot.values, hist2_slot.nvalues, hist2_weight, &proc, opr_codenum);
        }
    }

    // Process LHS histogram (excluding outlier endpoints) against RHS statistics
    if (hist1_exists && hist1_slot.nvalues > 2 && (mcv2_exists || hist2_exists))
    {
        double hist_selec_sum = 0.0;
        int k = (hist1_slot.nvalues - 3) / MAX_CONSIDERED_ELEMS + 1;  // Decimation step
        int n = 0;

        for (int i = 1; i < hist1_slot.nvalues - 1; i += k)
        {
            hist_selec_sum += inet_semi_join_sel(hist1_slot.values[i], mcv2_exists, mcv2_slot.values, mcv2_length, hist2_exists, hist2_slot.values, hist2_slot.nvalues, hist2_weight, &proc, opr_codenum);
            n++;
        }

        selec += (1.0 - nullfrac1 - sumcommon1) * hist_selec_sum / n;
    }

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