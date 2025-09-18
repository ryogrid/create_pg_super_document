# create_hashjoin_plan

## Location
src/backend/optimizer/plan/createplan.c: 4747 - 4935

## Overview
Creates a HashJoin plan node from a HashPath, implementing hash joins where the inner relation is used to build a hash table that is then probed by the outer relation.

## Definition


## Detailed Description
This function creates a HashJoin execution plan node from a HashPath. Hash joins are efficient when one relation (typically the smaller inner relation) can fit in a hash table built in memory, which is then probed by the outer relation to find matches. The function creates both a Hash node for building the hash table and a HashJoin node for the actual join operation. It handles hash key extraction from join clauses, sets up skew optimization for single-column joins when statistics are available, and manages batching for large datasets that don't fit in memory. The function also handles parallel execution by setting up shared hash table sizing information.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning context and state information
- : HashPath representing the chosen hash join access path with batching and hash clause information

## Dependencies
- Functions called/Symbols referenced:
  - build_path_tlist
  - create_plan_recurse
  - order_qual_clauses
  - IS_OUTER_JOIN
  - extract_actual_join_clauses
  - extract_actual_clauses
  - get_actual_clauses
  - list_difference
  - replace_nestloop_params
  - get_switched_clauses
  - is_opclause
  - lappend_oid
  - lsecond
  - make_hash
  - copy_plan_costsize
  - make_hashjoin
  - copy_generic_path_info
- Called from (representative examples):
  - create_join_plan

## Notes and Other Information
- Hash joins are typically the most efficient join method when one relation is much smaller than the other
- Creates separate Hash and HashJoin nodes - the Hash node builds the hash table from the inner relation
- Implements skew optimization for single-column joins when column statistics indicate data skew
- Handles batching for large datasets that exceed work_mem by spilling to disk
- Supports parallel execution with shared hash tables across multiple workers
- Extracts hash keys and operators needed for the hash table implementation
- Requests small target lists from inputs to minimize memory usage during hash table operations
- Located at src/backend/optimizer/plan/createplan.c:4747-4935
- Part of the JOIN METHODS section of the planner