# find_dependent_phvs_in_jointree

## Location
[src/backend/optimizer/prep/prepjointree.c:3901-3961](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L3901-L3961)

## Overview
Searches a specific jointree fragment and its referenced RTEs for PlaceHolderVars that depend on a given relation variable.

## Definition


## Detailed Description
This function performs a more targeted search than , focusing on a specific jointree fragment and its associated range table entries (RTEs). It operates in two phases: first checking the jointree fragment itself for references in join qualifiers, then examining each RTE referenced by the jointree fragment.

The function includes an optimization for LATERAL joins - it only checks RTEs that are marked as LATERAL, since non-LATERAL RTEs cannot contain cross-references to other RTEs by definition. This reduces unnecessary traversal of the range table.

Like its counterpart function, it includes an early optimization check for the existence of any PlaceHolderVars in the query before proceeding with the search.

## Parameters / Member Variables
- : PlannerInfo structure containing query planning state and parse tree
- : The specific jointree node/fragment to search within  
- : The relation variable number (RTE index) to search for dependencies on

## Dependencies
- Functions called/Symbols referenced:
  - find_dependent_phvs_context (context structure for walker)
  - [bms_make_singleton](../b/bms_make_singleton.md) (creates singleton bitmap set)
  - [find_dependent_phvs_walker](find_dependent_phvs_walker.md) (performs actual PHV dependency checking)
  - get_relids_in_jointree (extracts relation IDs from jointree fragment)
  - [bms_next_member](../b/bms_next_member.md) (iterates through bitmap set members)
  - rt_fetch (retrieves range table entry by ID)
  - range_table_entry_walker (traverses RTE structure)
- Called from (representative examples):
  - [remove_useless_results_recurse](../r/remove_useless_results_recurse.md) (in prepjointree.c:3544, 3646)

## Notes and Other Information
- This function is static and only used within prepjointree.c
- More focused than find_dependent_phvs, operating on specific jointree fragments rather than the entire parse tree
- Includes LATERAL join optimization - only checks RTEs marked as LATERAL since non-LATERAL RTEs cannot contain cross-references
- Two-phase approach: checks join qualifiers first, then individual RTEs
- Part of the query optimization process for identifying and removing unused result relations
- The jointree fragment check handles join conditions, while the RTE check handles individual table expressions