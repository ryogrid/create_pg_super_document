# targetIsInSortList

## Location
[src/backend/parser/parse_clause.c:3632-3658](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L3632-L3658)

## Overview
Checks whether a given target entry is already present in a SortGroupClause list, optionally matching a specific sort operator.

## Definition

```c
bool
targetIsInSortList(TargetEntry *tle, Oid sortop, List *sortList)
```
## Detailed Description
This function determines if a target entry is already represented in a sort or group list to prevent duplicate entries. It uses the ressortgroupref field of the TargetEntry to efficiently locate matching entries in the list. When a specific sort operator is provided, it also verifies operator compatibility, including checking for commutator operators (e.g., treating ASC and DESC as potentially redundant).

The function is designed to work with both ORDER BY and GROUP BY clauses. It deliberately ignores the nulls_first flag since different null ordering with the same sort operator is considered redundant. The function also handles the case where commutator operators might be equivalent for redundancy purposes.

## Parameters / Member Variables
- : Target entry to search for in the sort list
- : Sort operator OID to match (InvalidOid to ignore operator matching)
- : List of SortGroupClause nodes to search through

## Dependencies
- Functions called/Symbols referenced:
  - [get_commutator](../g/get_commutator.md)
- Called from (representative examples):
  - [addTargetToSortList](../a/addTargetToSortList.md)
  - [addTargetToGroupList](../a/addTargetToGroupList.md)  
  - [transformGroupClauseExpr](transformGroupClauseExpr.md)
  - [transformDistinctOnClause](transformDistinctOnClause.md)
  - [check_output_expressions](../c/check_output_expressions.md)
  - [targetIsInAllPartitionLists](targetIsInAllPartitionLists.md)

## Notes and Other Information
- Returns false immediately if tle->ressortgroupref is 0 (no marker assigned)
- Intentionally ignores nulls_first flag for redundancy checking
- Supports commutator operator matching for detecting equivalent sort orders
- Used for both sorting (ORDER BY) and grouping (GROUP BY) operations
- Critical for preventing duplicate entries in sort/group clause lists
- Efficient lookup using ressortgroupref rather than expensive expression comparison
- The reason this function exists instead of simple ressortgroupref checks is that a TLE might appear in only one of multiple related lists

## Simplified Source

```c
bool
targetIsInSortList(TargetEntry *tle, Oid sortop, List *sortList)
{
    Index ref = tle->ressortgroupref;
    ListCell *l;

    // Quick exit if target entry has no sort/group reference marker
    if (ref == 0)
        return false;

    // Search through the sort/group list for matching entries
    foreach(l, sortList)
    {
        SortGroupClause *scl = (SortGroupClause *) lfirst(l);

        // Check if reference matches and operator is compatible
        if (scl->tleSortGroupRef == ref &&
            (sortop == InvalidOid ||           // No specific operator required
             sortop == scl->sortop ||          // Exact operator match
             sortop == get_commutator(scl->sortop)))  // Commutator match (e.g., < vs >)
            return true;
    }

    return false;
}
```