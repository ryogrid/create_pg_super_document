# process_implied_equality

## Location
src/backend/optimizer/plan/initsplan.c: 2961 - 3099

## Overview
Creates a RestrictInfo clause representing an implied equality "item1 op item2" and integrates it into the query planner's constraint system, typically for equivalence class processing.

## Definition


## Detailed Description
This function constructs implied equality clauses that are derived from equivalence class relationships during query optimization. It builds a new OpExpr representing "item1 op item2" (typically a btree equality operator), performs constant folding if both expressions are constant, and integrates the resulting RestrictInfo into the planner's constraint lists.

The function handles several important aspects:
1. **Constant optimization**: When both operands are constants, it attempts to evaluate the expression at planning time
2. **Variable collection**: Identifies all relations referenced by the clause for proper distribution
3. **Pseudoconstant handling**: Manages variable-free clauses by placing them at appropriate join levels
4. **Merge join analysis**: Checks if the clause can be used for merge joins
5. **Target list management**: Ensures variables are available at join nodes

## Parameters / Member Variables
- : PlannerInfo structure containing global planner state
- : OID of the operator (typically a btree equality operator)
- : Collation OID for the comparison operation
- : Left operand expression (copied by the function)
- : Right operand expression (copied by the function)
- : Relids indicating the syntactic scope where clause should apply
- : Security level to assign to the new RestrictInfo
- : Whether both operands are known to be pseudo-constant

## Dependencies
- Functions called/Symbols referenced:
  - make_opclause (creates operator expression node)
  - copyObject (deep copies expression trees)
  - [eval_const_expressions](../e/eval_const_expressions.md) (performs constant folding)
  - [pull_varnos](pull_varnos.md) (extracts relation IDs from expressions)
  - [bms_is_subset](../b/bms_is_subset.md)/bms_is_empty (bitmap set operations)
  - [get_join_domain_min_rels](../g/get_join_domain_min_rels.md) (determines safe evaluation level)
  - [make_restrictinfo](../m/make_restrictinfo.md) (constructs RestrictInfo node)
  - [bms_membership](../b/bms_membership.md) (checks bitmap set cardinality)
  - [pull_var_clause](pull_var_clause.md) (extracts variable references)
  - [add_vars_to_targetlist](../a/add_vars_to_targetlist.md) (ensures vars available at join)
  - [check_mergejoinable](../c/check_mergejoinable.md) (analyzes merge join suitability)
  - [distribute_restrictinfo_to_rels](../d/distribute_restrictinfo_to_rels.md) (distributes clause to relation lists)

- Called from (representative examples):
  - [generate_base_implied_equalities_const](../g/generate_base_implied_equalities_const.md) (equivalence class constant processing)
  - [generate_base_implied_equalities_no_const](../g/generate_base_implied_equalities_no_const.md) (equivalence class non-constant processing)

## Notes and Other Information
- Returns NULL when constant folding produces TRUE (clause can be eliminated)
- Copies input expressions to avoid sharing substructure with originals
- Handles pseudoconstant clauses by setting appropriate join domain evaluation
- Caller responsible for equivalence class initialization via initialize_mergeclause_eclasses()
- Part of PostgreSQL's equivalence class system for transitive equality inference
- Critical for generating implied join conditions and optimizing complex queries
- Integrates with the broader constraint distribution system through distribute_restrictinfo_to_rels()