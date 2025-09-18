# networksel

## Location
src/backend/utils/adt/network_selfuncs.c: 79 - 195

## Overview
Calculates selectivity estimation for network subnet inclusion/overlap operators, used by the PostgreSQL query planner to estimate how many rows will match network-based WHERE clauses.

## Definition


## Detailed Description
The  function implements selectivity estimation for PostgreSQL's network data type operators (inet, cidr) when used in WHERE clauses with subnet inclusion and overlap operations. It analyzes column statistics to predict the fraction of rows that will satisfy conditions like  or .

The function follows a systematic approach: first checking if the expression is in the form , then utilizing most-common-values (MCV) statistics if available, and finally applying histogram-based estimation for the remaining population. This dual approach ensures accurate selectivity estimates across different data distributions.

The estimation process combines MCV selectivity (exact matches from frequent values) with histogram-based selectivity for less common values, weighted by their respective population fractions.

## Parameters / Member Variables
- : PlannerInfo pointer containing query planning context
- : OID of the network operator being evaluated  
- : List of arguments to the operator expression
- : Relation ID of the variable, or 0 if not restricted to a relation

## Dependencies
- Functions called/Symbols referenced:
  - get_restriction_variable
  - mcv_selectivity
  - [inet_opr_codenum](../i/inet_opr_codenum.md)
  - [inet_hist_value_sel](../i/inet_hist_value_sel.md)
  - [get_attstatsslot](../g/get_attstatsslot.md)
  - [fmgr_info](../f/fmgr_info.md)
  - [get_opcode](../g/get_opcode.md)
  - ReleaseVariableStats
  - DEFAULT_SEL
  - CLAMP_PROBABILITY
- Called from (representative examples):
  - Used as selectivity estimation function registered in system catalogs
  - Invoked by the query planner during optimization

## Notes and Other Information
- Returns default selectivity if the expression is not in the expected  format
- Handles NULL constants by returning 0.0 selectivity (no matches expected)
- Requires column statistics to provide meaningful estimates, falls back to defaults otherwise
- Uses operator-specific histogram analysis through 
- Combines MCV and histogram statistics with proper weighting by population fractions
- Results are clamped to valid probability range [0.0, 1.0]