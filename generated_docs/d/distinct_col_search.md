# distinct_col_search

## Location
[src/backend/optimizer/plan/analyzejoins.c:1144-1183](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/analyzejoins.c#L1144-L1183)

## Overview
A helper function that searches for a specific column number in a list of columns and returns the corresponding equality operator OID if found.

## Definition
```c
static Oid distinct_col_search(int colno, List *colnos, List *opids)
```

## Detailed Description
This function serves as a subroutine for `query_is_distinct_for()`, providing a simple lookup mechanism to find whether a specific column number exists in a list of target columns. When a match is found, it returns the corresponding equality operator OID from the parallel list.

The function performs a linear search through the column number list, and upon finding the first match, immediately returns the corresponding operator OID. If the column number is not found in the list, it returns `InvalidOid` to indicate no match.

This is used during distinctness analysis to check if columns from DISTINCT clauses, GROUP BY clauses, or set operations are present in the set of columns being tested for uniqueness, and to retrieve the appropriate equality operator for compatibility testing.

## Parameters / Member Variables
- `colno`: The target column number (resno) to search for
- `colnos`: List of integer column numbers to search within
- `opids`: Parallel list of equality operator OIDs corresponding to each column in colnos

## Dependencies
- Functions called/Symbols referenced:
  - forboth (PostgreSQL list iteration macro)
  - lfirst_int (extract integer from list cell)
  - lfirst_oid (extract OID from list cell)
  - InvalidOid (PostgreSQL invalid OID constant)
- Called from:
  - [query_is_distinct_for](../q/query_is_distinct_for.md) (multiple times at lines 1016, 1047, 1115)

## Notes and Other Information
- Declared as static, so it's only visible within the analyzejoins.c file
- If the colnos list contains duplicate entries, the function returns the operator OID corresponding to the first match
- The colnos and opids lists must be of equal length and maintain parallel correspondence
- Returns InvalidOid when the target column is not found, which callers use to detect missing columns
- Part of the distinctness analysis infrastructure that helps optimize queries by eliminating unnecessary joins

## Simplified Source

```c
static Oid
distinct_col_search(int colno, List *colnos, List *opids)
{
    ListCell *lc1, *lc2;

    // Search for colno in parallel lists
    forboth(lc1, colnos, lc2, opids)
    {
        if (colno == lfirst_int(lc1))
            return lfirst_oid(lc2);
    }

    return InvalidOid;
}
```