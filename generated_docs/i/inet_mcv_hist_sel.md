# inet_mcv_hist_sel

## Location
src/backend/utils/adt/network_selfuncs.c: 705 - 741

## Overview
Estimates join selectivity between a Most Common Values (MCV) list and a histogram for inet network operations by computing how each MCV value matches against the histogram distribution.

## Definition


## Detailed Description
This function performs selectivity estimation for join operations where one side has MCV statistics and the other has histogram statistics. It provides a hybrid approach that leverages the precision of MCV data for common values while using histogram estimation for the distribution analysis.

The algorithm processes each MCV value by:
1. Commuting the operator (negating opr_codenum) since inet_hist_value_sel expects the histogram on the left side
2. Calling inet_hist_value_sel to estimate what fraction of the histogram population would match the current MCV value
3. Scaling that fraction by the MCV value's frequency (mcv_numbers[i])
4. Accumulating these weighted contributions

The result represents the selectivity for the MCV portion of the join and still needs to be scaled according to the fraction of the right-hand side's population represented by the histogram.

## Parameters / Member Variables
- : Array of Datum values from the MCV list (left-hand side)
- : Array of frequency values corresponding to each MCV value
- : Number of entries in the MCV list
- : Array of Datum values representing histogram bucket boundaries (right-hand side)
- : Number of values in the histogram array
- : Operator code number for the join operation (gets commuted internally)

## Dependencies
- Functions called/Symbols referenced:
  - inet_hist_value_sel
- Called from (representative examples):
  - networkjoinsel_inner

## Notes and Other Information
- Commutes the operator by negating opr_codenum to match inet_hist_value_sel's expected parameter order
- More efficient than full histogram vs histogram comparison when one side has good MCV coverage
- The returned selectivity needs additional scaling based on the histogram's population coverage
- Combines the precision of MCV statistics with the distribution modeling of histograms
- Particularly effective for joins where one relation has skewed data with identifiable most common values