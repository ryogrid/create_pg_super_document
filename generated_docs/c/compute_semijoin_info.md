# compute_semijoin_info

## Location
[src/backend/optimizer/plan/initsplan.c:1700-1877](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L1700-L1877)

## Overview
Fills semijoin-related fields of a SpecialJoinInfo structure by analyzing whether the semijoin can be optimized using unique-ification techniques.

## Definition

```c
union(left_varnos, right_varnos);
```
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
  - [lappend_oid](../l/lappend_oid.md)
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

## Simplified Source

```c
static void
compute_semijoin_info(PlannerInfo *root, SpecialJoinInfo *sjinfo, List *clause)
{
    List *semi_operators = NIL;
    List *semi_rhs_exprs = NIL;
    bool all_btree = true;
    bool all_hash = enable_hashagg;
    ListCell *lc;

    // Initialize semijoin fields
    sjinfo->semi_can_btree = false;
    sjinfo->semi_can_hash = false;
    sjinfo->semi_operators = NIL;
    sjinfo->semi_rhs_exprs = NIL;

    // Only process semijoins
    if (sjinfo->jointype != JOIN_SEMI)
        return;

    // Analyze each clause for unique-ification potential
    foreach(lc, clause)
    {
        OpExpr *op = (OpExpr *) lfirst(lc);
        Oid opno;
        Node *left_expr, *right_expr;
        Relids left_varnos, right_varnos;

        // Must be binary equality operator
        if (!IsA(op, OpExpr) || list_length(op->args) != 2)
        {
            if (!clause_references_both_sides(op, sjinfo))
                continue;  // Ignore single-side clauses
            return;  // Complex clause, can't unique-ify
        }

        // Extract operator and expressions
        opno = op->opno;
        left_expr = linitial(op->args);
        right_expr = lsecond(op->args);
        left_varnos = pull_varnos(root, left_expr);
        right_varnos = pull_varnos(root, right_expr);

        // Determine which side is RHS (right-hand side of semijoin)
        if (bms_is_subset(right_varnos, sjinfo->syn_righthand) &&
            !bms_overlap(left_varnos, sjinfo->syn_righthand))
        {
            // Normal case: right_expr is RHS variable
        }
        else if (bms_is_subset(left_varnos, sjinfo->syn_righthand) &&
                 !bms_overlap(right_varnos, sjinfo->syn_righthand))
        {
            // Flipped case: commute operator and swap expressions
            opno = get_commutator(opno);
            if (!OidIsValid(opno))
                return;  // No commutator available
            right_expr = left_expr;
        }
        else
        {
            return;  // Mixed membership, can't unique-ify
        }

        // Check if operator supports btree or hash operations
        if (all_btree && !op_mergejoinable(opno, exprType(left_expr)))
            all_btree = false;
        if (all_hash && !op_hashjoinable(opno, exprType(left_expr)))
            all_hash = false;

        if (!(all_btree || all_hash))
            return;  // Neither method available

        // Collect operator and RHS expression
        semi_operators = lappend_oid(semi_operators, opno);
        semi_rhs_exprs = lappend(semi_rhs_exprs, copyObject(right_expr));
    }

    // Verify we found at least one unique-ifiable column
    if (semi_rhs_exprs == NIL || contain_volatile_functions((Node *) semi_rhs_exprs))
        return;

    // Success: set semijoin optimization info
    sjinfo->semi_can_btree = all_btree;
    sjinfo->semi_can_hash = all_hash;
    sjinfo->semi_operators = semi_operators;
    sjinfo->semi_rhs_exprs = semi_rhs_exprs;
}
```