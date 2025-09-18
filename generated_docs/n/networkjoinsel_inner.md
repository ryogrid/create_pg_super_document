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