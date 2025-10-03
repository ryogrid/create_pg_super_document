# indexcol_is_bool_constant_for_query

## Location
[src/backend/optimizer/path/indxpath.c:3614-3664](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L3614-L3664)

## Overview
Determines if an index column is constrained to have a constant value by boolean restriction clauses in the query's WHERE conditions.

## Definition

```c
bool
indexcol_is_bool_constant_for_query(PlannerInfo *root,
									IndexOptInfo *index,
									int indexcol)
```
## Detailed Description
This function addresses a specific optimization scenario for boolean index columns. When a boolean column is constrained by WHERE conditions like "WHERE boolcol" or "WHERE NOT boolcol", expression preprocessing simplifies these to boolean expressions rather than explicit equality comparisons like "WHERE boolcol = true". This means no EquivalenceClass is created for the constant value, which would normally signal that the column is irrelevant for sort-order considerations.

The function specifically handles this case by checking if a boolean index column matches any boolean restriction clauses, allowing such columns to be recognized as effectively constant for query optimization purposes, just as non-boolean columns with explicit "col = constant" restrictions are handled.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing global query information
- `*index`: IndexOptInfo structure representing the index being analyzed
- `indexcol`: Column number within the index to check for boolean constant constraints
## Dependencies
- Functions called/Symbols referenced:
  - [IsBooleanOpfamily](../I/IsBooleanOpfamily.md)
  - [match_boolean_index_clause](../m/match_boolean_index_clause.md)
  - [IndexOptInfo](../I/IndexOptInfo.md) (structure)
  - [RestrictInfo](../R/RestrictInfo.md) (structure)
- Called from (representative examples):
  - [build_index_pathkeys](../b/build_index_pathkeys.md)

## Notes and Other Information
- Only applicable to boolean opfamily index columns (checked via IsBooleanOpfamily)
- Designed to complement the standard EquivalenceClass-based constant detection for non-boolean columns
- Skips pseudoconstant restriction clauses to avoid wasting cycles on negligible match possibilities
- Uses match_boolean_index_clause to perform the actual clause-to-column matching
- Returns true if any boolean restriction clause constrains the specified index column
- Helps ensure boolean index columns receive the same optimization treatment as other data types
- File location: src/backend/optimizer/path/indxpath.c:3614-3664

## Simplified Source

```c
bool
indexcol_is_bool_constant_for_query(PlannerInfo *root,
                                   IndexOptInfo *index,
                                   int indexcol)
{
    ListCell *lc;

    // Only boolean index columns can be handled this way
    if (!IsBooleanOpfamily(index->opfamily[indexcol]))
        return false;

    // Check each restriction clause for the index's relation
    foreach(lc, index->rel->baserestrictinfo)
    {
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

        // Skip pseudoconstants (unlikely to match, not worth the cycles)
        if (rinfo->pseudoconstant)
            continue;

        // Check if this boolean clause constrains our index column
        if (match_boolean_index_clause(root, rinfo, indexcol, index))
            return true;
    }

    return false;
}
```