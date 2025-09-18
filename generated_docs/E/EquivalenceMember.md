# EquivalenceMember

## Location
src/include/nodes/pathnodes.h: 1430 - 1444

## Overview
EquivalenceMember represents one member expression of an EquivalenceClass in PostgreSQL's query optimizer, storing expressions that are known to be equal and can be substituted for each other during query planning.

## Definition


## Detailed Description
EquivalenceMember is a fundamental structure in PostgreSQL's query optimizer that represents individual expressions within an EquivalenceClass. An EquivalenceClass groups expressions that are known to be equal due to equality constraints (e.g., WHERE clauses or JOIN conditions), allowing the optimizer to substitute equivalent expressions during planning.

The structure supports special handling for child relations in inheritance hierarchies through the em_is_child flag. Child members are derived by transposing parent relation expressions for appendrel children and are used for determining pathkeys and building MergeAppend paths. These child members don't impact the EC's overall properties and are essentially "reflections" of real members.

The em_datatype field handles binary-compatible operator families where the expression's datatype might differ from the operator family's expected type, which is crucial for operations like anyarray_ops.

## Parameters / Member Variables
- : Standard NodeTag for PostgreSQL node identification
- : The actual expression represented by this equivalence member
- : Set of relation IDs that appear in the expression, used for join planning
- : True if the expression is a pseudoconstant (doesn't vary within query execution)
- : True if this is a derived version for an appendrel child relation
- : The nominal datatype used by the operator family (may differ from em_expr's type)
- : The join domain containing the source equality clause that created this member
- : Link to the corresponding parent EquivalenceMember when em_is_child is true

## Dependencies
- Functions called/Symbols referenced:
  - JoinDomain (for join domain tracking)
  - Expr (base expression type)
  - Relids (relation ID set)
  - NodeTag (PostgreSQL node system)

- Called from (representative examples):
  - process_equivalence (equivalence class processing)
  - add_eq_member (adding new equivalence members)
  - find_ec_member_matching_expr (finding matching expressions)
  - generate_base_implied_equalities_const (generating implied equalities)
  - create_join_clause (creating join clauses from equivalences)

## Notes and Other Information
- Child members (em_is_child=true) should be ignored by most EquivalenceClass operations
- Child members never affect ec_has_const or ec_has_volatile flags of their parent EC
- The em_datatype field is essential for binary-compatible operator families
- Used extensively in pathkey generation, join planning, and merge join optimization
- Part of PostgreSQL's sophisticated query optimization framework for equality reasoning