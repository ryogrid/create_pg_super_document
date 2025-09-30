# reconsider_outer_join_clause

## Location
[src/backend/optimizer/path/equivclass.c:2114-2236](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L2114-L2236)

## Overview
Processes a single LEFT/RIGHT JOIN clause to determine if constant values can be safely propagated from the outer relation to the inner relation through equivalence classes.

## Definition
```c
static bool reconsider_outer_join_clause(PlannerInfo *root, OuterJoinClauseInfo *ojcinfo, bool outer_on_left)
```

## Detailed Description
This function implements the core logic for optimizing LEFT and RIGHT JOIN clauses by leveraging transitivity in equivalence relationships. Given an outer join clause OUTERVAR = INNERVAR, it searches for existing equivalence classes where OUTERVAR = CONSTANT, enabling the safe derivation of INNERVAR = CONSTANT constraints that can be pushed into the inner relation.

The function extracts the outer and inner variables based on the join direction, then searches through all equivalence classes for matches. For each matching equivalence class containing constants, it generates new equality clauses between the inner variable and each constant, using select_equality_operator to find appropriate operators and build_implied_join_equality to construct the RestrictInfo structures.

The optimization is safe because any inner rows not meeting the constant constraint cannot contribute to the join result anyway, as they would be filtered out by the corresponding outer relation constraint.

## Parameters / Member Variables
- `root`: Pointer to the PlannerInfo containing global planning state
- `ojcinfo`: OuterJoinClauseInfo structure containing the outer join clause details and associated special join information
- `outer_on_left`: Boolean indicating whether the outer relation is on the left side of the join clause

## Dependencies
- Functions called/Symbols referenced:
  - [is_opclause](../i/is_opclause.md) (checks if clause is an operator expression)
  - [op_input_types](../o/op_input_types.md) (extracts operator input data types)
  - [get_leftop](../g/get_leftop.md), get_rightop (extract operands from expressions)
  - [select_equality_operator](../s/select_equality_operator.md) (finds suitable equality operators)
  - [build_implied_join_equality](../b/build_implied_join_equality.md) (constructs new RestrictInfo clauses)
  - [find_join_domain](../f/find_join_domain.md) (locates appropriate join domain)
  - [process_equivalence](../p/process_equivalence.md) (processes the new equivalence relationship)
  - [equal](../e/equal.md) (tests expression equality)
  - [bms_copy](../b/bms_copy.md) (copies relation bitmaps)
- Called from (representative examples):
  - [reconsider_outer_join_clauses](reconsider_outer_join_clauses.md) (main outer join processing loop)

## Notes and Other Information
- Returns true if constant propagation was successful, false otherwise
- Only processes equivalence classes that contain constants (ec_has_const = true)
- Avoids volatile equivalence classes to maintain correctness
- Validates semantic compatibility through collation and operator family matching
- Generates constraints within the appropriate JoinDomain for the outer join
- Each successful constant propagation enables the parent function to remove the original outer join clause
- The function ensures that at least one constant constraint is successfully generated before declaring success

## Simplified Source

```c
static bool
reconsider_outer_join_clause(PlannerInfo *root, OuterJoinClauseInfo *ojcinfo,
                             bool outer_on_left)
{
    RestrictInfo *rinfo = ojcinfo->rinfo;
    SpecialJoinInfo *sjinfo = ojcinfo->sjinfo;
    Expr *outervar, *innervar;
    Oid opno, collation, left_type, right_type, inner_datatype;
    Relids inner_relids;
    ListCell *lc1;

    Assert(is_opclause(rinfo->clause));
    opno = ((OpExpr *) rinfo->clause)->opno;
    collation = ((OpExpr *) rinfo->clause)->inputcollid;

    // Extract operands based on join direction
    op_input_types(opno, &left_type, &right_type);
    if (outer_on_left)
    {
        outervar = (Expr *) get_leftop(rinfo->clause);
        innervar = (Expr *) get_rightop(rinfo->clause);
        inner_datatype = right_type;
        inner_relids = rinfo->right_relids;
    }
    else
    {
        outervar = (Expr *) get_rightop(rinfo->clause);
        innervar = (Expr *) get_leftop(rinfo->clause);
        inner_datatype = left_type;
        inner_relids = rinfo->left_relids;
    }

    // Search equivalence classes for outer variable match
    foreach(lc1, root->eq_classes)
    {
        EquivalenceClass *cur_ec = (EquivalenceClass *) lfirst(lc1);
        bool match = false;
        ListCell *lc2;

        // Skip non-constant or volatile ECs
        if (!cur_ec->ec_has_const || cur_ec->ec_has_volatile)
            continue;
        // Check semantic compatibility
        if (collation != cur_ec->ec_collation ||
            !equal(rinfo->mergeopfamilies, cur_ec->ec_opfamilies))
            continue;

        // Look for outer variable in this EC
        foreach(lc2, cur_ec->ec_members)
        {
            EquivalenceMember *cur_em = (EquivalenceMember *) lfirst(lc2);

            Assert(!cur_em->em_is_child);
            if (equal(outervar, cur_em->em_expr))
            {
                match = true;
                break;
            }
        }
        if (!match)
            continue;

        // Generate inner variable = constant constraints
        match = false;
        foreach(lc2, cur_ec->ec_members)
        {
            EquivalenceMember *cur_em = (EquivalenceMember *) lfirst(lc2);
            Oid eq_op;
            RestrictInfo *newrinfo;
            JoinDomain *jdomain;

            if (!cur_em->em_is_const)
                continue;

            eq_op = select_equality_operator(cur_ec, inner_datatype, cur_em->em_datatype);
            if (!OidIsValid(eq_op))
                continue;

            newrinfo = build_implied_join_equality(root, eq_op, cur_ec->ec_collation,
                                                   innervar, cur_em->em_expr,
                                                   bms_copy(inner_relids),
                                                   cur_ec->ec_min_security);
            jdomain = find_join_domain(root, sjinfo->syn_righthand);
            if (process_equivalence(root, &newrinfo, jdomain))
                match = true;
        }

        // Return success if any constant constraint was generated
        if (match)
            return true;
        else
            break;  // Outer variable appears in at most one EC
    }

    return false;
}
```