# process_security_barrier_quals

## Location
[src/backend/optimizer/plan/initsplan.c:1272-1321](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L1272-L1321)

## Overview
Transfers security-barrier qualifiers from a RangeTblEntry's securityQuals field into the relation's baserestrictinfo list during query planning.

## Definition


## Detailed Description
The  function handles the processing of security-barrier conditions that were previously placed by the rewriter into the RTE's securityQuals field. It transfers these conditions into the relation's baserestrictinfo for proper handling during query optimization.

The function processes security quals in levels, where each sublist of clauses gets assigned incrementally higher security levels. This ensures that security-barrier views are properly enforced with the correct precedence. In inheritance scenarios, it only processes quals attached to the parent relation, as these will be valid for all child relations and can be used for equivalence class creation.

## Parameters / Member Variables
- : PlannerInfo structure containing global planning state and context
- : Range table index identifying the specific relation being processed
- : JoinTreeItem representing the relation's position in the join tree structure

## Dependencies
- Functions called/Symbols referenced:
  - [distribute_quals_to_rels](../d/distribute_quals_to_rels.md)
  - [JoinTreeItem](../J/JoinTreeItem.md) (struct)
- Called from (representative examples):
  - [deconstruct_distribute](../d/deconstruct_distribute.md)

## Notes and Other Information
- The function uses a "cheat" by passing ojscope = qualscope instead of NULL to force Var-free qualifiers to be evaluated at the relation level rather than being pushed to the top of the tree
- Security levels are incremented for each sublist of security qualifiers to maintain proper security barrier enforcement
- An assertion ensures that the security_level used doesn't exceed root->qual_security_level
- Each element in securityQuals is an implicitly-ANDed list of clauses that should receive the same security level