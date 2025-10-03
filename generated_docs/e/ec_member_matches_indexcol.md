# ec_member_matches_indexcol

## Location
[src/backend/optimizer/path/indxpath.c:3382-3439](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/indxpath.c#L3382-L3439)

## Overview
Tests whether an EquivalenceClass member matches an index column for generating implied equalities during query optimization.

## Definition

```c
static bool
ec_member_matches_indexcol(PlannerInfo *root, RelOptInfo *rel,
						   EquivalenceClass *ec, EquivalenceMember *em,
						   void *arg)
```
## Detailed Description
This function serves as a callback for  to determine if a specific EquivalenceClass member can be matched against an index column. It performs compatibility checks between the equivalence member and the target index column, considering operator family compatibility (for btree indexes), collation matching, and operand structure matching.

For btree indexes, the function enforces strict opfamily compatibility since no clause generated from an incompatible EC could be used with the index. For non-btree indexes, opfamily checking is skipped due to the difficulty of determining clause compatibility, though this may result in false positives that require later verification.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing global query information
- `*rel`: RelOptInfo structure representing the relation
- `*ec`: EquivalenceClass being tested for compatibility
- `*em`: EquivalenceMember within the equivalence class to test
- `*arg`: Void pointer to ec_member_matches_arg structure containing index and column information
## Dependencies
- Functions called/Symbols referenced:
  - [list_member_oid](../l/list_member_oid.md)
  - IndexCollMatchesExprColl
  - [match_index_to_operand](../m/match_index_to_operand.md)
  - [EquivalenceClass](../E/EquivalenceClass.md) (structure)
  - [EquivalenceMember](../E/EquivalenceMember.md) (structure)
  - [IndexOptInfo](../I/IndexOptInfo.md) (structure)
  - ec_member_matches_arg (structure)
  - BTREE_AM_OID (constant)
- Called from (representative examples):
  - [match_eclass_clauses_to_index](../m/match_eclass_clauses_to_index.md)
  - Used as callback in ec_member_matches_arg

## Notes and Other Information
- Designed specifically as a callback function for equivalence class processing
- Enforces collation matching for all index types regardless of access method
- For btree indexes, performs strict opfamily compatibility checking
- For non-btree indexes, may return false positives that require later verification
- The arg parameter must be cast to ec_member_matches_arg structure to access index and indexcol fields
- Returns true only if all compatibility checks pass and the member expression matches the index operand
- File location: src/backend/optimizer/path/indxpath.c:3382-3439

## Simplified Source

```c
static bool
ec_member_matches_indexcol(PlannerInfo *root, RelOptInfo *rel,
                           EquivalenceClass *ec, EquivalenceMember *em,
                           void *arg)
{
    // Extract index and column info from callback argument
    IndexOptInfo *index = ((ec_member_matches_arg *) arg)->index;
    int indexcol = ((ec_member_matches_arg *) arg)->indexcol;
    Oid curFamily = index->opfamily[indexcol];
    Oid curCollation = index->indexcollations[indexcol];

    Assert(indexcol < index->nkeycolumns);

    // For btree indexes, check operator family compatibility
    // Non-btree indexes skip this check due to complexity
    if (index->relam == BTREE_AM_OID &&
        !list_member_oid(ec->ec_opfamilies, curFamily))
        return false;

    // All index types require collation match
    if (!IndexCollMatchesExprColl(curCollation, ec->ec_collation))
        return false;

    // Final check: does the member expression match the index operand?
    return match_index_to_operand((Node *) em->em_expr, indexcol, index);
}
```