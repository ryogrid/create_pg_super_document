# mcv_combine_selectivities

## Location
[src/backend/statistics/mcv.c:2006-2047](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/mcv.c#L2006-L2047)

## Overview
Combines per-column and multi-column MCV selectivity estimates to produce a unified selectivity estimate that accounts for both MCV-covered and non-MCV-covered portions of the data.

## Definition
```c
Selectivity mcv_combine_selectivities(Selectivity simple_sel,
                                     Selectivity mcv_sel,
                                     Selectivity mcv_basesel,
                                     Selectivity mcv_totalsel)
```

## Detailed Description
This function combines different types of selectivity estimates to produce an accurate overall selectivity estimate. It takes a simple selectivity estimate (computed assuming column independence) and corrects it using MCV statistics. The function calculates the selectivity for data not covered by MCV items (other_sel) and combines it with the selectivity for MCV-covered data (mcv_sel). The approach recognizes that MCV lists may not cover 100% of the data and handles the statistical correction appropriately by treating (mcv_sel - mcv_basesel) as a correction factor for the MCV-covered portion.

## Parameters / Member Variables
- `simple_sel`: Simple selectivity estimate computed without extended statistics, assuming column independence
- `mcv_sel`: Sum of frequencies of all matching MCV items
- `mcv_basesel`: Sum of base frequencies of all matching MCV items
- `mcv_totalsel`: Sum of frequencies of all MCV items (not just matching ones), used as an upper bound

## Dependencies
- Functions called/Symbols referenced:
  - CLAMP_PROBABILITY
- Called from (representative examples):
  - statext_mcv_clauselist_selectivity (multiple calls)

## Notes and Other Information
- The function implements a sophisticated statistical model that accounts for partial MCV coverage
- Simple selectivity generally satisfies (simple_sel >= mcv_basesel) due to MCV list limitations
- The difference (simple_sel - mcv_basesel) estimates the selectivity for data not covered by MCV
- The correction (mcv_sel - mcv_basesel) adjusts for the MCV-covered portion
- Uses probability clamping to ensure results stay within valid [0,1] range
- Critical component of PostgreSQL's extended statistics system for multi-column correlation handling
- Located in src/backend/statistics/mcv.c:2006-2047