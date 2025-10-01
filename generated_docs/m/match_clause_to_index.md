# match_clause_to_index

## Location
[src/backend/optimizer/path/indxpath.c:2084-2202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L2084-L2202)

## Overview
Tests whether a qualification clause can be used with an index and adds appropriate IndexClause entries to the clause set if usable.

## Definition
static void match_clause_to_index(PlannerInfo *root, RestrictInfo *rinfo, IndexOptInfo *index, IndexClauseSet *clauseset)

## Detailed Description
This is the core function that determines whether a specific restriction clause (WHERE condition or join condition) can be effectively utilized by a given index. The function performs several validation steps: it rejects pseudoconstant clauses, checks security restrictions using restriction_is_securely_promotable(), and then systematically tests each index key column for compatibility with the clause using match_clause_to_indexcol(). The function includes important optimizations such as duplicate detection (preventing the same RestrictInfo from being added multiple times) and first-match selection (avoiding inflated selectivity estimates when a clause could match multiple index columns). If a match is found, it creates an IndexClause entry and adds it to the appropriate column list in the IndexClauseSet.

## Parameters / Member Variables
- `root`: PlannerInfo structure containing global query planning information and context
- `rinfo`: RestrictInfo node representing the qualification clause to be tested
- `index`: IndexOptInfo structure containing detailed information about the index being evaluated
- `clauseset`: IndexClauseSet structure where matching clauses will be stored, organized by index column

## Dependencies
- Functions called/Symbols referenced:
  - [restriction_is_securely_promotable](../r/restriction_is_securely_promotable.md)
  - [match_clause_to_indexcol](match_clause_to_indexcol.md)
  - [IndexClause](../I/IndexClause.md)
  - [IndexOptInfo](../I/IndexOptInfo.md)
  - IndexClauseSet
- Called from (representative examples):
  - [match_join_clauses_to_index](match_join_clauses_to_index.md)
  - [match_clauses_to_index](match_clauses_to_index.md)
  - ec_member_matches_arg

## Notes and Other Information
- This is a static function, accessible only within the indxpath.c file
- The function includes important security checks to prevent unsafe clause promotion in row-level security scenarios
- Implements duplicate detection using pointer equality to avoid redundant IndexClause entries
- Uses first-match semantics when a clause could potentially match multiple index columns
- The function can handle expression indexes and partial indexes with appropriate safety checks
- Part of PostgreSQL's sophisticated index selection and optimization system
- Location: src/backend/optimizer/path/indxpath.c:2084-2202

## Simplified Source

```c
static void
match_clause_to_index(PlannerInfo *root, RestrictInfo *rinfo,
                      IndexOptInfo *index, IndexClauseSet *clauseset)
{
    int indexcol;

    // Skip pseudoconstant clauses (constants can't use indexes effectively)
    if (rinfo->pseudoconstant)
        return;

    // Check security restrictions - some clauses must wait for others
    if (!restriction_is_securely_promotable(rinfo, index->rel))
        return;

    // Try to match clause against each index key column
    for (indexcol = 0; indexcol < index->nkeycolumns; indexcol++) {
        IndexClause *iclause;
        ListCell *lc;

        // Check for duplicates - avoid adding same clause twice
        foreach(lc, clauseset->indexclauses[indexcol]) {
            iclause = (IndexClause *) lfirst(lc);
            if (iclause->rinfo == rinfo)
                return; // Already processed this clause
        }

        // Attempt to match clause to this specific index column
        iclause = match_clause_to_indexcol(root, rinfo, indexcol, index);
        if (iclause) {
            // Success - add to appropriate column list
            clauseset->indexclauses[indexcol] =
                lappend(clauseset->indexclauses[indexcol], iclause);
            clauseset->nonempty = true;
            return; // Use first match only to avoid selectivity inflation
        }
    }
}
```