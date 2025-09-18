# tsquerysel

## Location
src/backend/tsearch/ts_selfuncs.c: 150 - 206

## Overview
Computes selectivity estimates for tsvector variables against tsquery constants by analyzing statistics and query structure.

## Definition


## Detailed Description
 is the core selectivity estimation function for text search operations. It calculates the probability that a tsvector column will match a given TSQuery expression. The function uses PostgreSQL's column statistics, particularly the most-common-elements (MCELEM) statistics, to make informed estimates about query selectivity.

The function follows a multi-tiered approach:
1. First checks if the TSQuery is empty (returns 0.0 selectivity)
2. If statistics are available, uses most-common-elements data via 
3. Falls back to structure-based estimation via  when no stats are available
4. Adjusts for null fraction in the column

This function is essential for the PostgreSQL query planner to make cost-based decisions for full-text search queries.

## Parameters / Member Variables
- : Pointer to VariableStatData containing column statistics and metadata
- : Datum representing the TSQuery constant to match against

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetTSQuery: Converts Datum to TSQuery structure
  - get_attstatsslot: Retrieves statistics slot data for the column
  - mcelem_tsquery_selec: Calculates selectivity using most-common-elements statistics
  - tsquery_opr_selec_no_stats: Fallback selectivity calculation without statistics
  - free_attstatsslot: Releases memory for statistics slot data
- Data structures used:
  - TSQuery: Text search query structure
  - Form_pg_statistic: PostgreSQL column statistics structure
  - AttStatsSlot: Statistics slot containing values and frequencies
- Constants used:
  - STATISTIC_KIND_MCELEM: Statistics type for most-common-elements
  - ATTSTATSSLOT_VALUES/ATTSTATSSLOT_NUMBERS: Flags for statistics retrieval
- Called from (representative examples):
  - tsmatchsel: Main selectivity function for @@ operator

## Notes and Other Information
- Returns 0.0 selectivity for empty TSQuery expressions
- Adjusts final selectivity by the null fraction () from column statistics
- Uses a static function scope, indicating it's an internal helper function
- The function assumes that MCELEM statistics contain TEXT elements for tsvector columns
- Gracefully degrades when statistics are unavailable, using structure-based estimation
- Part of PostgreSQL's advanced text search selectivity estimation infrastructure