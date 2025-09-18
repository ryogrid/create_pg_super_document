# set_base_rel_sizes

## Location
[src/backend/optimizer/path/allpaths.c:290-332](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/allpaths.c#L290-L332)

## Overview
Sets the size estimates (rows and widths) for each base-relation entry and determines whether to consider parallel paths for base relations.

## Definition


## Detailed Description
This function performs a separate pass over all base relations to establish size estimates and parallel processing flags before path generation begins. It ensures that rowcount estimates are available for parameterized path generation and that each relation's consider_parallel flag is correctly set. 

The function iterates through the simple_rel_array, processing only base relations (RELOPT_BASEREL) and skipping other relation types. For each valid base relation, it first determines parallelism eligibility if parallel mode is enabled globally, then calls set_rel_size() to establish the actual size estimates.

The sequencing is critical: parallel considerations must be evaluated before set_rel_size() because inheritance parents may have their consider_parallel flag modified during append relation processing, and some RTE types immediately create paths during size estimation.

## Parameters / Member Variables
- : PlannerInfo structure containing global optimizer state, including the simple_rel_array and simple_rte_array

## Dependencies
- Functions called/Symbols referenced:
  - RELOPT_BASEREL (enum value)
  - [set_rel_consider_parallel](set_rel_consider_parallel.md)
  - [set_rel_size](set_rel_size.md)
- Called from (representative examples):
  - [make_one_rel](../m/make_one_rel.md)

## Notes and Other Information
- Located in src/backend/optimizer/path/allpaths.c:290-332
- Static function, only used within the allpaths.c module
- Performs a separate pass specifically to ensure proper sequencing of size estimation and parallel flag setting
- Includes safeguards to skip non-baserel RTEs and empty array slots
- Critical for establishing the foundation for subsequent path generation phases
- The parallelism check precedes size estimation to handle inheritance parents and immediate path creation scenarios