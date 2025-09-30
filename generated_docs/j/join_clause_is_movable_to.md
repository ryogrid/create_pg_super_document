# join_clause_is_movable_to

## Location
[src/backend/optimizer/util/restrictinfo.c:584-669](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/restrictinfo.c#L584-L669)

## Overview
Tests whether a join clause is a safe candidate for parameterization of a scan on a specified base relation by evaluating various safety conditions for clause movement.

## Definition

```c
union of currentrelids and the required_outer
 *		relids (parameterization's outer relations)
 *
 * The API would be a bit clearer if we passed the current relids and the
 * outer relids separately and did bms_union internally;
```
## Detailed Description
This function determines whether a join clause can safely be evaluated at a relation below its normal semantic level (i.e., its required_relids), provided that values of variables from other relations are supplied as parameters. The function implements several safety checks to ensure that moving the clause will not change query semantics or produce incorrect results.

Key safety conditions checked:
1. The clause must physically reference the target relation to prevent undesirable movement of degenerate join clauses
2. The clause cannot be moved into the non-nullable side of an outer join, as this would suppress rows rather than null-extending them
3. There must not be any outer join below the clause that would null variables from the target relation
4. The clause must not use relations that have LATERAL references to the target relation
5. Clone versions of outer-join clauses are rejected to avoid generating redundant parameterized paths

## Parameters / Member Variables
- : RestrictInfo structure containing the join clause and its metadata
- : RelOptInfo structure representing the base relation being considered as the target for clause movement

## Dependencies
- Functions called/Symbols referenced:
  - [bms_is_member](../b/bms_is_member.md) (bitmap set membership test)
  - [bms_overlap](../b/bms_overlap.md) (bitmap set overlap test)
- Called from (representative examples):
  - [match_join_clauses_to_index](../m/match_join_clauses_to_index.md)
  - [check_index_predicates](../c/check_index_predicates.md)  
  - [BuildParameterizedTidPaths](../B/BuildParameterizedTidPaths.md)
  - [extract_restriction_or_clauses](../e/extract_restriction_or_clauses.md)
  - make_simple_restrictinfo

## Notes and Other Information
The function is part of PostgreSQL's query optimization infrastructure for creating parameterized paths. It works in conjunction with join_clause_is_movable_into to determine optimal clause placement. The rejection of is_clone versions prevents the optimizer from generating multiple parameterized paths that differ only in which outer joins null the parameterization relations, as one path from the minimally-parameterized has_clone version is sufficient for optimization purposes.

## Simplified Source

```c
bool
join_clause_is_movable_to(RestrictInfo *rinfo, RelOptInfo *baserel)
{
    // Clause must physically reference the target relation
    if (!bms_is_member(baserel->relid, rinfo->clause_relids))
        return false;

    // Cannot move outer-join clause into the join's outer side
    if (bms_is_member(baserel->relid, rinfo->outer_relids))
        return false;

    // Target rel's Vars must not be nulled by any outer join
    if (bms_overlap(rinfo->clause_relids, baserel->nulling_relids))
        return false;

    // Clause must not use rels with LATERAL references to this rel
    if (bms_overlap(baserel->lateral_referencers, rinfo->clause_relids))
        return false;

    // Ignore clone clauses
    if (rinfo->is_clone)
        return false;

    return true;
}
```