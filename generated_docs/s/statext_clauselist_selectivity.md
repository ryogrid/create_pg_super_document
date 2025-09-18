# statext_clauselist_selectivity

## Location
src/backend/statistics/extended_stats.c: 1984 - 2034

## Overview
Estimates the selectivity of a list of clauses using the best available multi-column statistics, combining MCV (Most Common Values) lists and functional dependencies for accurate selectivity estimation.

## Definition


## Detailed Description
This function provides sophisticated selectivity estimation by leveraging PostgreSQL's extended statistics infrastructure. It follows a two-phase approach: first attempting to estimate clauses using multivariate MCV lists for exact selectivity values, then applying functional dependencies to remaining clauses for additional correlation information. The function prioritizes more complex statistics (MCV lists) over simpler ones (functional dependencies) because complex stats can track more detailed correlations between attributes and are considered more reliable. For OR clauses, only MCV estimation is performed since functional dependencies only work with AND-connected clauses.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning context and information
- : List of restriction clauses to estimate selectivity for
- : Relation ID for single-relation queries, or 0 for join queries
- : Type of join operation being performed
- : Special join information for outer joins and semi-joins
- : RelOptInfo structure for the relation being planned
- : Output parameter - bitmapset of clauses that were successfully estimated
- : Boolean flag indicating whether clauses are connected by OR (true) or AND (false)

## Dependencies
- Functions called/Symbols referenced:
  - statext_mcv_clauselist_selectivity
  - dependencies_clauselist_selectivity
  - JoinType
  - SpecialJoinInfo
- Called from (representative examples):
  - clauselist_selectivity_ext
  - clauselist_selectivity_or

## Notes and Other Information
The function implements a layered approach to selectivity estimation where more sophisticated statistics methods are tried first. MCV lists can provide exact selectivity for specific value combinations, while functional dependencies provide information about attribute correlation strength. The function is designed to handle both AND and OR clause combinations, but functional dependencies are only applicable to AND clauses due to their mathematical properties. The estimatedclauses parameter allows callers to track which clauses have been processed to avoid double-counting in subsequent estimation steps.