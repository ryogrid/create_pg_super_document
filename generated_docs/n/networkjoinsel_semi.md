# networkjoinsel_semi

## Location
src/backend/utils/adt/network_selfuncs.c: 390 - 538

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