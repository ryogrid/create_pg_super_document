# searchForDefault

## Location
[src/backend/rewrite/rewriteHandler.c:1289-1314](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L1289-L1314)

## Overview
Checks whether a VALUES range table entry contains any SetToDefault items in its value lists.

## Definition
```c
static bool searchForDefault(RangeTblEntry *rte)
```

## Detailed Description
This utility function scans through a VALUES range table entry's value lists to determine if any of the values are SetToDefault nodes. SetToDefault nodes represent columns where the user has explicitly specified DEFAULT in a VALUES clause (e.g., `INSERT INTO table VALUES (1, DEFAULT, 3)`).

The function performs a nested iteration:
1. Iterates through each value list in the VALUES RTE (each list represents one row of values)
2. For each value list, iterates through each individual value (column)
3. Checks if any value is a SetToDefault node

This information is crucial for the rewrite system to determine whether default value processing will be needed during query execution. If no DEFAULT keywords are present, certain optimizations can be applied.

## Parameters / Member Variables
- `rte`: The RangeTblEntry representing a VALUES clause to search through

## Dependencies
- Functions called/Symbols referenced:
  - [SetToDefault](../S/SetToDefault.md)
- Called from (representative examples):
  - [rewriteValuesRTE](../r/rewriteValuesRTE.md)

## Notes and Other Information
- Returns true as soon as the first SetToDefault node is found (early termination)
- Returns false if no SetToDefault nodes are found in any value list
- Used as an optimization check in the rewrite phase
- Part of the VALUES clause processing logic in the query rewriter
- Helps determine whether default value expansion is necessary

## Simplified Source

```c
static bool searchForDefault(RangeTblEntry *rte) {
    // Search through each row in the VALUES clause
    foreach(lc, rte->values_lists) {
        List *sublist = (List *) lfirst(lc);

        // Check each column value in this row
        foreach(lc2, sublist) {
            Node *col = (Node *) lfirst(lc2);

            // Return true immediately if DEFAULT keyword found
            if (IsA(col, SetToDefault))
                return true;
        }
    }

    // No DEFAULT keywords found in any values
    return false;
}
```