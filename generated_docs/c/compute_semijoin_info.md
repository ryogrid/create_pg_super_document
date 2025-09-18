# compute_semijoin_info

## Location
src/backend/optimizer/plan/initsplan.c: 1700 - 1877

## Overview
Fills semijoin-related fields of a SpecialJoinInfo structure by analyzing whether the semijoin can be optimized using unique-ification techniques.

## Definition


## Detailed Description
The  function analyzes semijoin operations to determine if they can be optimized through unique-ification of the right-hand side relations. This optimization is crucial for improving the performance of EXISTS subqueries and IN clauses that are converted to semijoins.

The function examines the join conditions to identify whether they consist of AND'ed equality operators with RHS variables on one side. If such a pattern is found, the function determines whether the unique-ification can be performed using btree or hash operations, which enables the optimizer to use more efficient join algorithms.

The analysis involves:
1. Parsing each clause to identify binary equality operators
2. Checking that one side contains only RHS variables and the other side contains only LHS variables
3. Verifying that the operators support either btree (merge join) or hash join operations
4. Ensuring that the expressions to be unique-ified are not volatile

## Parameters / Member Variables
- : PlannerInfo structure containing global planning state and optimizer information
- : SpecialJoinInfo structure to be populated with semijoin metadata (only jointype and syn_righthand fields need to be set)
- : List of join condition clauses syntactically associated with the semijoin

## Dependencies
- Functions called/Symbols referenced:
  - [pull_varnos](../p/pull_varnos.md)
  - [contain_volatile_functions](contain_volatile_functions.md)
  - [get_commutator](../g/get_commutator.md)
  - [op_mergejoinable](../o/op_mergejoinable.md)
  - [get_mergejoin_opfamilies](../g/get_mergejoin_opfamilies.md)
  - [op_hashjoinable](../o/op_hashjoinable.md)
  - lappend_oid
  - copyObject
  - bms_* (various bitmap set operations)
- Called from (representative examples):
  - [make_outerjoininfo](../m/make_outerjoininfo.md)

## Notes and Other Information
- The function only processes semijoins (JOIN_SEMI); other join types are ignored
- The analysis focuses on syntactically-associated clauses, which may include clauses that aren't semantically associated with the join
- Clauses that reference only one side of the join are ignored unless they contain volatile functions
- The function requires that operators be either all btree-compatible or all hash-compatible for unique-ification
- Cross-type operators are supported, with the assumption that the corresponding single-type operator will be available at execution time
- The enable_hashagg setting affects whether hash-based unique-ification is considered
- If successful, the function populates semi_can_btree, semi_can_hash, semi_operators, and semi_rhs_exprs fields in the SpecialJoinInfo structure
- This information is later used by create_unique_plan() to implement the unique-ification optimization