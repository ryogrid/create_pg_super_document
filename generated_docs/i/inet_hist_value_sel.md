# inet_hist_value_sel

## Location
[src/backend/utils/adt/network_selfuncs.c:604-672](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_selfuncs.c#L604-L672)

## Overview
Estimates the selectivity of histogram values against a single constant value for inet subnet inclusion/overlap operators, calculating the fraction of histogram population that satisfies "value OPR CONST".

## Definition


## Detailed Description
This function performs selectivity estimation for inet types by analyzing histogram buckets against a constant value. It's specifically designed for subnet inclusion operators (<<, <<=, >>, >>=, &&) and uses a sophisticated bucket matching algorithm.

The function implements two forms of bucket matching:
1. **Full bucket match**: When the constant matches both bucket endpoints, the entire bucket is considered matched (contributes 1.0 to selectivity)
2. **Partial bucket match**: When the constant matches only one endpoint or falls between endpoints, a fractional contribution is calculated using a power-of-two divider based on the difference between decisive bits and common address bits

The algorithm handles mixed address families (IPv4/IPv6) and varying network mask lengths (masklens) by using boundary-specific dividers and taking the maximum divider value to minimize estimation errors in buckets with disparate masklens.

For performance optimization, when there are too many histogram elements, the function decimates the data by examining every k-th element where k = (nvalues - 2) / MAX_CONSIDERED_ELEMS + 1.

## Parameters / Member Variables
- : Array of Datum values representing histogram bucket boundaries
- : Number of values in the histogram array
- : The constant inet value to compare against (right-hand side of operator)
- : Operator code number indicating the type of inclusion/overlap operation

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInetPP](../D/DatumGetInetPP.md)
  - [inet_inclusion_cmp](inet_inclusion_cmp.md)
  - [inet_hist_match_divider](inet_hist_match_divider.md)
  - MAX_CONSIDERED_ELEMS
- Called from (representative examples):
  - [networksel](../n/networksel.md)
  - [inet_mcv_hist_sel](inet_mcv_hist_sel.md)
  - [inet_hist_inclusion_join_sel](inet_hist_inclusion_join_sel.md)
  - [inet_semi_join_sel](inet_semi_join_sel.md)

## Notes and Other Information
- Returns 0.0 for histograms with <= 1 values to guard against division by zero
- The partial match divider calculation is empirical and may be subject to change with further experimentation
- Designed to handle the complexity of inet btree comparison where network part is the major sort key
- The algorithm is unfair when both boundary dividers are valid but far apart, but this is considered a rare situation
- For buckets with different address families on boundaries, only the matching family boundary is considered