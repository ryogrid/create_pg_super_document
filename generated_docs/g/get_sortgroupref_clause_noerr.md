# get_sortgroupref_clause_noerr

## Location
[src/backend/optimizer/util/tlist.c:443-462](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/tlist.c#L443-L462)

## Overview
Like get_sortgroupref_clause, but returns NULL rather than throwing an error if the SortGroupRef is not found.

## Definition
SortGroupClause *get_sortgroupref_clause_noerr(Index sortref, List *clauses)

## Detailed Description
This function provides a non-error variant of get_sortgroupref_clause(). It searches through a list of SortGroupClause structures to find the one whose tleSortGroupRef field matches the provided sortref index. However, unlike its counterpart, if no matching clause is found, this function returns NULL instead of raising an error. This makes it suitable for situations where the absence of a matching clause is an acceptable outcome that should be handled gracefully rather than treated as an error condition.

## Parameters / Member Variables
- `sortref`: Index value (SortGroupRef) to search for in the clauses
- `clauses`: List of SortGroupClause structures to search within

## Dependencies
- Functions called/Symbols referenced:
  - SortGroupClause (structure type)
  - Index (type alias)
- Called from (representative examples):
  - [group_keys_reorder_by_pathkeys](group_keys_reorder_by_pathkeys.md)
  - [make_group_input_target](../m/make_group_input_target.md)
  - [make_partial_grouping_target](../m/make_partial_grouping_target.md)

## Notes and Other Information
This function is particularly useful in optimization scenarios where the presence or absence of a sort/group clause affects the chosen strategy, but the absence shouldn't be treated as an error. For example, when reordering group keys by path keys or when constructing partial grouping targets, the optimizer may need to check if certain clauses exist without assuming they must be present. The function follows the same linear search pattern as get_sortgroupref_clause() but provides gentler error handling semantics.