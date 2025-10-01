# match_pathkeys_to_index

## Location
[src/backend/optimizer/path/indxpath.c:3020-3129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L3020-L3129)

## Overview
Matches pathkey ordering requirements to index columns to generate ORDER BY expressions that can be satisfied using index scans instead of explicit sorting.

## Definition
```c
static void
match_pathkeys_to_index(IndexOptInfo *index, List *pathkeys,
                       List **orderby_clauses_p,
                       List **clause_columns_p)
```

## Detailed Description
This function analyzes whether a given set of pathkeys (representing desired sort order) can be satisfied by scanning an index in its natural order. It examines each pathkey in the requested ordering and attempts to match it to an index column that can provide the required sort behavior.

For indexes that support ordering operations (amcanorderbyop), the function builds ORDER BY expressions of the form "indexedcol operator pseudoconstant" for distance-based ordering (such as nearest-neighbor searches in GiST indexes). It only considers ascending sort order with nulls last, as these are the standard semantics. The function allows partial matches, returning clauses for all pathkeys that could be matched, enabling callers to determine if a full or partial ordering can be achieved through index scanning.

## Parameters / Member Variables
- `index`: IndexOptInfo structure containing metadata about the candidate index
- `pathkeys`: List of PathKey structures representing the desired sort order
- `orderby_clauses_p`: Output parameter for list of ORDER BY expressions that can use the index
- `clause_columns_p`: Output parameter for list of integers indicating which index columns correspond to each clause

## Dependencies
- Functions called/Symbols referenced:
  - lfirst
  - [bms_equal](../b/bms_equal.md)
  - [match_clause_to_ordering_op](match_clause_to_ordering_op.md)
  - [lappend](../l/lappend.md)
  - [lappend_int](../l/lappend_int.md)
- Called from (representative examples):
  - [build_index_paths](../b/build_index_paths.md)

## Notes and Other Information
- Only works with indexes that have the amcanorderbyop property (like GiST)
- Requires pathkeys to request BTLessStrategyNumber (ascending) sort with nulls last
- Rejects pathkeys with volatile expressions since they cannot be indexed reliably
- Allows any index column to match any pathkey position (non-sequential matching)
- Supports both regular and child equivalence class members for maximum flexibility
- Returns partial matches when only some pathkeys can be satisfied by the index
- Essential for optimizing ORDER BY clauses using index scans instead of explicit sorting
- Particularly important for nearest-neighbor and distance-based queries in spatial indexes
- Enables significant performance improvements by eliminating separate sort operations

## Simplified Source

```c
static void
match_pathkeys_to_index(IndexOptInfo *index, List *pathkeys,
                       List **orderby_clauses_p, List **clause_columns_p)
{
    *orderby_clauses_p = NIL;
    *clause_columns_p = NIL;

    // Only indexes supporting ordering operations are useful
    if (!index->amcanorderbyop)
        return;

    foreach(lc1, pathkeys)
    {
        PathKey *pathkey = (PathKey *) lfirst(lc1);
        bool found = false;

        // Must be ascending sort with nulls last
        if (pathkey->pk_strategy != BTLessStrategyNumber ||
            pathkey->pk_nulls_first)
            return;

        // Skip volatile expressions
        if (pathkey->pk_eclass->ec_has_volatile)
            return;

        // Try to match pathkey to index columns
        foreach(lc2, pathkey->pk_eclass->ec_members)
        {
            EquivalenceMember *member = (EquivalenceMember *) lfirst(lc2);

            // Skip if member references other relations
            if (!bms_equal(member->em_relids, index->rel->relids))
                continue;

            // Check each index column for a match
            for (int indexcol = 0; indexcol < index->nkeycolumns; indexcol++)
            {
                Expr *expr = match_clause_to_ordering_op(index, indexcol,
                                                        member->em_expr,
                                                        pathkey->pk_opfamily);
                if (expr)
                {
                    *orderby_clauses_p = lappend(*orderby_clauses_p, expr);
                    *clause_columns_p = lappend_int(*clause_columns_p, indexcol);
                    found = true;
                    break;
                }
            }

            if (found)
                break;
        }

        // Return partial matches if this pathkey couldn't be matched
        if (!found)
            return;
    }
}
```