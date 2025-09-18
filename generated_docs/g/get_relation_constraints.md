# get_relation_constraints

## Location
[src/backend/optimizer/util/plancat.c:1267-1386](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/util/plancat.c#L1267-L1386)

## Overview
Retrieves and processes all applicable constraint expressions for a given relation, including check constraints, NOT NULL constraints, and partition constraints.

## Definition


## Detailed Description
The  function extracts constraint expressions from a relation and transforms them into a standardized format suitable for query optimization. It processes three types of constraints: check constraints, NOT NULL constraints, and partitioning constraints.

For check constraints, the function validates each constraint (skipping unvalidated ones), converts the stored binary representation to expression trees using , and applies canonicalization and constant simplification. The expressions are normalized to use the correct varno for easy comparison with WHERE clause expressions.

For NOT NULL constraints, when requested, the function generates explicit "IS NOT NULL" expressions for each non-dropped attribute marked as . For partition constraints, it includes the partitioning constraints if the relation is a partition.

All constraint expressions undergo the same preprocessing as qual clauses in  to ensure proper matching during query optimization.

## Parameters / Member Variables
- : PlannerInfo context containing planner state information
- : OID of the relation to extract constraints from
- : RelOptInfo structure representing the relation in the optimizer
- : Whether to include constraints marked NO INHERIT
- : Whether to generate explicit NOT NULL constraint expressions
- : Whether to include partitioning constraints for partitioned tables

## Dependencies
- Functions called/Symbols referenced:
  - [TupleConstr](../T/TupleConstr.md)
  - [stringToNode](../s/stringToNode.md)
  - [eval_const_expressions](../e/eval_const_expressions.md)
  - [canonicalize_qual](../c/canonicalize_qual.md)
  - [ChangeVarNodes](../C/ChangeVarNodes.md)
  - [list_concat](../l/list_concat.md)
  - [make_ands_implicit](../m/make_ands_implicit.md)
  - NullTest
  - makeVar
  - [set_baserel_partition_constraint](../s/set_baserel_partition_constraint.md)
- Called from (representative examples):
  - [relation_excluded_by_constraints](../r/relation_excluded_by_constraints.md)

## Notes and Other Information
- This is a static function, not part of the external API
- Assumes the relation is already safely locked by the caller
- Currently invoked at most once per relation per planner run for performance
- Skips unvalidated check constraints for correctness
- Converts expressions to implicit-AND format (List) for easier processing
- For composite columns, argisrow=false is used since attnotnull represents IS DISTINCT FROM NULL rather than SQL-spec IS NOT NULL
- The function handles varno adjustment to ensure constraint expressions reference the correct relation