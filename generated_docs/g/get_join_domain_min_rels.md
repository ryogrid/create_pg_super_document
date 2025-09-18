# get_join_domain_min_rels

## Location
src/backend/optimizer/plan/initsplan.c: 3169 - 3208

## Overview
Identifies the appropriate join level for derived quals belonging to a join domain by removing lower outer joins that could potentially commute out of the domain.

## Definition


## Detailed Description
This function addresses a specific optimization challenge in PostgreSQL's query planner. When deriving pseudoconstant (Var-free) clauses from EquivalenceClasses, the ideal approach would be to apply these clauses at the top level of the EC's join domain. However, complications arise when outer joins inside that domain get commuted with joins outside it, making it difficult to find the correct placement for the clause.

To solve this issue, the function removes any lower outer joins from the relid set and applies the clause to just the remaining relations. This approach still produces correct results because if the clause evaluates to FALSE, the left-hand side of these joins will be empty, leading to an empty join result overall.

The function includes an important optimization: if the join domain is the top-level join domain of the query, there's no need to remove outer joins since there's nothing else to commute with.

## Parameters / Member Variables
- : PlannerInfo structure containing global information about the query being planned
- : Relids bitmap representing the relations in the join domain for which we want to find the minimum set

## Dependencies
- Functions called/Symbols referenced:
  - [bms_copy](../b/bms_copy.md) (bitmap copy operation)
  - [bms_equal](../b/bms_equal.md) (bitmap equality comparison)
  - [bms_is_member](../b/bms_is_member.md) (bitmap membership test)
  - [bms_del_member](../b/bms_del_member.md) (remove single member from bitmap)
  - [bms_del_members](../b/bms_del_members.md) (remove multiple members from bitmap)
  - [SpecialJoinInfo](../S/SpecialJoinInfo.md) (structure for outer join information)
  - JOIN_LEFT (enum value for left outer joins)
- Called from:
  - [process_implied_equality](../p/process_implied_equality.md)

## Notes and Other Information
- The result is always freshly palloc'd; the input domain_relids is not modified
- This function cannot be used in distribute_qual_to_rels where it deals with pseudoconstant quals because the necessary SpecialJoinInfos aren't all formed at that point
- The function specifically targets LEFT joins when looking for lower outer joins that could potentially commute out
- This is a static function within the initsplan.c module, indicating it's an internal optimization utility