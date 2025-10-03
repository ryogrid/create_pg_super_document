# assignSortGroupRef

## Location
[src/backend/parser/parse_clause.c:3591-3631](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_clause.c#L3591-L3631)

## Overview
Assigns an unused ressortgroupref number to a target entry if it doesn't already have one, ensuring unique reference numbers for sorting and grouping operations.

## Definition

```c
Index
assignSortGroupRef(TargetEntry *tle, List *tlist)
```
## Detailed Description
This function manages the assignment of unique reference numbers (ressortgroupref) to target entries in PostgreSQL's query processing. These reference numbers are essential for linking target list entries with their corresponding sort and group clauses during query execution planning.

The function uses a simple strategy to ensure uniqueness: it scans the entire target list to find the maximum existing reference number and assigns the next available number (max + 1) to the target entry. If the target entry already has a reference number, it returns the existing one without modification.

## Parameters / Member Variables
- `*tle`: Target entry that needs a ressortgroupref assignment
- `*tlist`: Target list containing the target entry and other entries to check for existing reference numbers
## Dependencies
- Functions called/Symbols referenced:
  - No external function calls (uses only basic list operations)
- Called from (representative examples):
  - [addTargetToSortList](addTargetToSortList.md)
  - [addTargetToGroupList](addTargetToGroupList.md)
  - [transformDistinctOnClause](../t/transformDistinctOnClause.md)
  - [create_unique_plan](../c/create_unique_plan.md)
  - [build_minmax_path](../b/build_minmax_path.md)
  - [generate_setop_child_grouplist](../g/generate_setop_child_grouplist.md)

## Notes and Other Information
- Simple but effective algorithm ensures no duplicate reference numbers
- Reference numbers start from 1 (maxRef + 1 where initial maxRef is 0)
- The ressortgroupref field in TargetEntry serves as a unique identifier for sort/group operations
- Used extensively throughout the planner to correlate target list entries with sort/group clauses
- Performance is O(n) where n is the size of the target list, but target lists are typically small
- Critical for proper execution of ORDER BY, GROUP BY, and DISTINCT operations

## Simplified Source

```c
Index
assignSortGroupRef(TargetEntry *tle, List *tlist)
{
    // Return existing reference if already assigned
    if (tle->ressortgroupref)
        return tle->ressortgroupref;

    // Find the maximum reference number in the target list
    Index maxRef = 0;
    foreach(cell, tlist)
    {
        Index ref = ((TargetEntry *) lfirst(cell))->ressortgroupref;
        if (ref > maxRef)
            maxRef = ref;
    }

    // Assign next available reference number
    tle->ressortgroupref = maxRef + 1;
    return tle->ressortgroupref;
}
```