# apply_child_basequals

## Location
src/backend/optimizer/util/inherit.c: 842 - 968

## Overview
Translates and applies base restriction qualifiers from a parent relation to a child relation, optimizing constant expressions and handling security qualifiers.

## Definition


## Detailed Description
This function is responsible for propagating restriction qualifiers (WHERE clause conditions) from a parent relation to its child relations in inheritance hierarchies or partitioned tables. It translates variable references using the append relation mapping, evaluates constant expressions for optimization opportunities, and handles security qualifiers.

The function performs several key optimizations: it evaluates expressions that become constants after translation, removes conditions that are always true, and identifies when conditions are always false (indicating the child relation can be excluded from the query). It also processes security qualifiers that may be specific to individual child relations, particularly in UNION ALL scenarios.

The function returns false if any qualifier evaluates to constant false or NULL, signaling that the child relation should be marked as dummy since it contributes no rows to the query result.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and context
- : RelOptInfo for the parent relation containing base restriction info
- : RelOptInfo for the child relation to receive translated qualifiers  
- : RangeTblEntry for the child relation
- : AppendRelInfo containing variable translation mappings between parent and child

## Dependencies
- Functions called/Symbols referenced:
  - [adjust_appendrel_attrs](adjust_appendrel_attrs.md) (translates variable references)
  - [eval_const_expressions](../e/eval_const_expressions.md) (evaluates constant expressions)
  - [make_ands_implicit](../m/make_ands_implicit.md) (flattens AND clauses)
  - [contain_vars_of_level](../c/contain_vars_of_level.md) (checks for variables)
  - [contain_volatile_functions](../c/contain_volatile_functions.md) (checks for volatile functions)
  - [make_restrictinfo](../m/make_restrictinfo.md) (creates RestrictInfo structures)
  - [restriction_is_always_false](../r/restriction_is_always_false.md) (tests for unsatisfiable conditions)
  - [restriction_is_always_true](../r/restriction_is_always_true.md) (tests for tautological conditions)
  - [AppendRelInfo](../A/AppendRelInfo.md) (data structure for relation mapping)
- Called from (representative examples):
  - [build_simple_rel](../b/build_simple_rel.md)

## Notes and Other Information
- Returns false to indicate child relation should be treated as dummy (excluded from scan)
- Handles pseudoconstant qualifiers that don't contain variables but may contain non-volatile functions
- Processes security qualifiers specific to child relations (mainly for UNION ALL subqueries)
- Tracks minimum security level among all applied qualifiers
- Sets root->hasPseudoConstantQuals flag when pseudoconstant conditions are found
- Performs constant folding optimization to eliminate unnecessary conditions
- Part of PostgreSQL's inheritance and partitioning optimization system
- Located in src/backend/optimizer/util/inherit.c at lines 842-968