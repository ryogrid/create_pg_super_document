# process_equivalence

## Location
[src/backend/optimizer/path/equivclass.c:117-470](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L117-L470)

## Overview
Processes equivalence clauses with mergejoinable operators to build or update EquivalenceClass structures that represent transitively equal expressions in the query optimizer.

## Definition

```c
bool
process_equivalence(PlannerInfo *root,
					RestrictInfo **p_restrictinfo,
					JoinDomain *jdomain)
```
## Detailed Description
This function is a core component of PostgreSQL's query optimizer equivalence class system. It analyzes equality clauses (e.g., a.col = b.col) and determines if they can be represented as EquivalenceClasses, which enable the optimizer to understand that certain expressions are transitively equal and can be used interchangeably for optimization purposes.

The function implements a UNION-FIND-like algorithm to manage equivalence relationships. It handles four main scenarios when processing a new equivalence clause:
1. Both expressions already exist in the same EquivalenceClass (no action needed)
2. Both expressions exist in different EquivalenceClasses (merge the classes)
3. One expression exists in an EquivalenceClass (add the other to the same class)
4. Neither expression exists (create a new EquivalenceClass)

The function also performs important transformations, such as converting X=X clauses into IS NOT NULL tests when the operator is strict, which provides better selectivity estimates.

Security considerations are built in - the function rejects clauses containing leaky functions when security_level > 0 to ensure proper evaluation timing.

## Parameters / Member Variables
- : PlannerInfo structure containing global optimizer state and equivalence class lists
- : Pointer to RestrictInfo representing the equality clause being processed; may be modified to point to a transformed clause
- : JoinDomain limiting the applicability of deductions from the EquivalenceClass

## Dependencies
- Functions called/Symbols referenced:
  - [canonicalize_ec_expression](../c/canonicalize_ec_expression.md)
  - [add_eq_member](../a/add_eq_member.md)
  - [is_opclause](../i/is_opclause.md)
  - [get_leftop](../g/get_leftop.md)/get_rightop
  - [op_input_types](../o/op_input_types.md)
  - [equal](../e/equal.md)
  - [set_opfuncid](../s/set_opfuncid.md)
  - [func_strict](../f/func_strict.md)
  - [make_restrictinfo](../m/make_restrictinfo.md)
  - [list_concat](../l/list_concat.md)
  - [bms_join](../b/bms_join.md)
- Called from (representative examples):
  - [distribute_qual_to_rels](../d/distribute_qual_to_rels.md)
  - [reconsider_outer_join_clause](../r/reconsider_outer_join_clause.md)
  - [reconsider_full_join_clause](../r/reconsider_full_join_clause.md)

## Notes and Other Information
- Only called during planner startup, not during GEQO exploration
- Implements equivalence class merging with proper handling of canonical state constraints
- Returns true if the clause was successfully processed as an equivalence, false if it should be treated as an ordinary restriction clause
- Initializes left_ec/right_ec fields in the RestrictInfo to point to the representing EquivalenceClass
- The algorithm could be optimized with better data structures than simple lists if it becomes a performance bottleneck

## Simplified Source

```c
bool
process_equivalence(PlannerInfo *root,
                   RestrictInfo **p_restrictinfo,
                   JoinDomain *jdomain)
{
    RestrictInfo *restrictinfo = *p_restrictinfo;
    Expr *clause = restrictinfo->clause;
    Expr *item1, *item2;
    EquivalenceClass *ec1 = NULL, *ec2 = NULL;
    EquivalenceMember *em1, *em2;

    // Reject security-sensitive clauses that aren't leakproof
    if (restrictinfo->security_level > 0 && !restrictinfo->leakproof)
        return false;

    // Extract operator info and canonicalize expressions
    Oid opno = ((OpExpr *) clause)->opno;
    Oid collation = ((OpExpr *) clause)->inputcollid;
    item1 = canonicalize_ec_expression(get_leftop(clause), ...);
    item2 = canonicalize_ec_expression(get_rightop(clause), ...);
    List *opfamilies = restrictinfo->mergeopfamilies;

    // Handle X=X clauses by converting to IS NOT NULL if operator is strict
    if (equal(item1, item2)) {
        if (func_strict(opfuncid)) {
            // Convert to NullTest and update restrictinfo
            *p_restrictinfo = make_restrictinfo_for_nulltest(...);
        }
        return false;
    }

    // Search existing EquivalenceClasses for matches
    foreach(lc1, root->eq_classes) {
        EquivalenceClass *cur_ec = (EquivalenceClass *) lfirst(lc1);

        // Skip volatile ECs and check collation/opfamily compatibility
        if (cur_ec->ec_has_volatile ||
            collation != cur_ec->ec_collation ||
            !equal(opfamilies, cur_ec->ec_opfamilies))
            continue;

        // Look for matching expressions in this EC
        foreach(lc2, cur_ec->ec_members) {
            EquivalenceMember *cur_em = (EquivalenceMember *) lfirst(lc2);

            if (!ec1 && equal(item1, cur_em->em_expr)) {
                ec1 = cur_ec; em1 = cur_em;
            }
            if (!ec2 && equal(item2, cur_em->em_expr)) {
                ec2 = cur_ec; em2 = cur_em;
            }
        }

        if (ec1 && ec2) break;
    }

    // Handle the four cases based on what we found:

    if (ec1 && ec2) {
        if (ec1 == ec2) {
            // Case 1: Both in same EC - just add source
            ec1->ec_sources = lappend(ec1->ec_sources, restrictinfo);
        } else {
            // Case 2: Different ECs - merge them
            ec1->ec_members = list_concat(ec1->ec_members, ec2->ec_members);
            ec1->ec_sources = list_concat(ec1->ec_sources, ec2->ec_sources);
            ec1->ec_relids = bms_join(ec1->ec_relids, ec2->ec_relids);
            ec2->ec_merged = ec1;
            root->eq_classes = list_delete_nth_cell(root->eq_classes, ec2_idx);
        }

        // Mark restrictinfo as associated with final EC
        restrictinfo->left_ec = restrictinfo->right_ec = ec1;
        restrictinfo->left_em = em1;
        restrictinfo->right_em = em2;

    } else if (ec1) {
        // Case 3: Add item2 to ec1
        em2 = add_eq_member(ec1, item2, item2_relids, jdomain, NULL, item2_type);
        ec1->ec_sources = lappend(ec1->ec_sources, restrictinfo);
        restrictinfo->left_ec = restrictinfo->right_ec = ec1;
        restrictinfo->left_em = em1;
        restrictinfo->right_em = em2;

    } else if (ec2) {
        // Case 3: Add item1 to ec2
        em1 = add_eq_member(ec2, item1, item1_relids, jdomain, NULL, item1_type);
        ec2->ec_sources = lappend(ec2->ec_sources, restrictinfo);
        restrictinfo->left_ec = restrictinfo->right_ec = ec2;
        restrictinfo->left_em = em1;
        restrictinfo->right_em = em2;

    } else {
        // Case 4: Create new two-entry EC
        EquivalenceClass *ec = makeNode(EquivalenceClass);
        ec->ec_opfamilies = opfamilies;
        ec->ec_collation = collation;
        ec->ec_sources = list_make1(restrictinfo);

        em1 = add_eq_member(ec, item1, item1_relids, jdomain, NULL, item1_type);
        em2 = add_eq_member(ec, item2, item2_relids, jdomain, NULL, item2_type);

        root->eq_classes = lappend(root->eq_classes, ec);
        restrictinfo->left_ec = restrictinfo->right_ec = ec;
        restrictinfo->left_em = em1;
        restrictinfo->right_em = em2;
    }

    return true;
}
```