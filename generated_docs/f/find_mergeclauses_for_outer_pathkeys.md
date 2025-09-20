# find_mergeclauses_for_outer_pathkeys

## Location
[src/backend/optimizer/path/pathkeys.c:1524-1638](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/pathkeys.c#L1524-L1638)

## Overview
This function attempts to find a list of mergeclauses that can be used with a specified ordering for the join's outer relation in merge join operations.

## Definition

```c
structs the inner pathkeys
		 * list, and we also have to deal with such cases specially in
		 * create_mergejoin_plan().
		 *----------
		 */
		foreach(j, restrictinfos)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(j);
			EquivalenceClass *clause_ec;

			clause_ec = rinfo->outer_is_left ?
				rinfo->left_ec : rinfo->right_ec;
			if (clause_ec == pathkey_ec)
				matched_restrictinfos = lappend(matched_restrictinfos, rinfo);
		}

		/*
		 * If we didn't find a mergeclause, we're done --- any additional
		 * sort-key positions in the pathkeys are useless.  (But we can still
		 * mergejoin if we found at least one mergeclause.)
		 */
		if (matched_restrictinfos == NIL)
			break;
```
## Detailed Description
The function takes a pathkeys list showing the ordering of an outer-rel path and attempts to match mergejoinable restriction clauses to create a maximal list of usable mergeclauses. The algorithm iterates through each pathkey and finds all restriction clauses that have the same equivalence class as the pathkey. 

Key behaviors:
- Ensures equivalence classes are cached in the clauses before processing
- Matches mergejoin clauses with pathkeys based on equivalence classes
- Handles multiple matching clauses for the same pathkey (common in outer-join scenarios)
- Stops processing when no mergeclause is found for a pathkey position
- Returns mergeclauses ordered to match the input pathkeys for execution

The function is designed to handle complex scenarios including outer joins where multiple clauses might match the same pathkey, and it prioritizes finding any valid merge plan over optimizing clause ordering.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context information
- : A pathkeys list showing the ordering of an outer-rel path
- : List of mergejoinable restriction clauses for the join relation, marked with outer_is_left to show which side is associated with the outer path

## Dependencies
- Functions called/Symbols referenced:
  - [update_mergeclause_eclasses](../u/update_mergeclause_eclasses.md)
  - PathKey
  - EquivalenceClass
  - [list_concat](../l/list_concat.md)
- Called from (representative examples):
  - [sort_inner_and_outer](../s/sort_inner_and_outer.md) (src/backend/optimizer/path/joinpath.c:1401)
  - [generate_mergejoin_paths](../g/generate_mergejoin_paths.md) (src/backend/optimizer/path/joinpath.c:1494)

## Notes and Other Information
- The restrictinfos must be pre-marked via outer_is_left to indicate which side of each clause is associated with the current outer path
- Returns NIL if no merge can be done, otherwise returns a maximal list of usable mergeclauses
- The result list is ordered to match the pathkeys as required for execution
- Can handle non-canonical ordering of pathkeys for the inner side, which may occur in complex join scenarios
- Designed to work with equivalence-class processing that removes redundant mergeclauses in simple inner-join cases