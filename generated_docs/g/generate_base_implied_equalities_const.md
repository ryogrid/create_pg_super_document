# generate_base_implied_equalities_const

## Location
[src/backend/optimizer/path/equivclass.c:1108-1202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L1108-L1202)

## Overview
Generates implied equality clauses for equivalence classes that contain pseudoconstants by creating "member = const" restrictions for each non-constant member.

## Definition

```c
static void
generate_base_implied_equalities_const(PlannerInfo *root,
									   EquivalenceClass *ec)
```
## Detailed Description
This function handles the specific case where an EquivalenceClass contains one or more constant or pseudoconstant members. It implements an optimization strategy that generates equality clauses comparing each variable member to a chosen constant member, effectively constraining all variables at their points of creation without requiring variable-to-variable comparisons.

The function employs a preference hierarchy when selecting the constant member, favoring actual constants over pseudoconstants (such as Params) because constraint exclusion machinery can better optimize "var = const" equalities compared to "var = param" expressions.

For the trivial case of exactly two members with one source clause (a simple "var = const"), the function optimizes by reusing the original clause rather than rebuilding an equivalent one.

## Parameters / Member Variables
- `*root`: PlannerInfo structure containing planner state information
- `*ec`: EquivalenceClass containing constant members to process
## Dependencies
- Functions called/Symbols referenced:
  - [distribute_restrictinfo_to_rels](../d/distribute_restrictinfo_to_rels.md)
  - [select_equality_operator](../s/select_equality_operator.md)
  - [process_implied_equality](../p/process_implied_equality.md)
- Called from (representative examples):
  - [generate_base_implied_equalities](generate_base_implied_equalities.md)

## Notes and Other Information
- Prefers actual Const nodes over other pseudoconstants for better constraint exclusion
- Handles the trivial two-member, one-source case by reusing the original RestrictInfo
- Uses the constant's em_jdomain as qualscope for generated clauses
- Marks the EC as broken (ec_broken = true) if required equality operators are not available
- Generated clauses are stored in ec_derives for potential selectivity estimation use
- Sets mergejoinable clause markings (left_ec, right_ec, left_em, right_em) for non-degenerate clauses
- Does not generate join clauses since ec_has_const eclasses are not used for joins
- Located in src/backend/optimizer/path/equivclass.c:1108-1202

## Simplified Source

```c
static void
generate_base_implied_equalities_const(PlannerInfo *root, EquivalenceClass *ec)
{
    EquivalenceMember *const_em = NULL;
    ListCell *lc;

    // Trivial case: single "var = const" clause, reuse original
    if (list_length(ec->ec_members) == 2 && list_length(ec->ec_sources) == 1)
    {
        RestrictInfo *restrictinfo = (RestrictInfo *) linitial(ec->ec_sources);
        distribute_restrictinfo_to_rels(root, restrictinfo);
        return;
    }

    // Find the best constant member (prefer Const over pseudoconstants)
    foreach(lc, ec->ec_members)
    {
        EquivalenceMember *cur_em = (EquivalenceMember *) lfirst(lc);

        if (cur_em->em_is_const)
        {
            const_em = cur_em;
            if (IsA(cur_em->em_expr, Const))
                break;  // Actual constant is preferred
        }
    }

    // Generate "member = const" equality for each non-constant member
    foreach(lc, ec->ec_members)
    {
        EquivalenceMember *cur_em = (EquivalenceMember *) lfirst(lc);
        Oid eq_op;
        RestrictInfo *rinfo;

        if (cur_em == const_em)
            continue;

        // Find equality operator between member and constant
        eq_op = select_equality_operator(ec, cur_em->em_datatype, const_em->em_datatype);
        if (!OidIsValid(eq_op))
        {
            ec->ec_broken = true;
            break;
        }

        // Create the implied equality clause
        rinfo = process_implied_equality(root, eq_op, ec->ec_collation,
                                         cur_em->em_expr, const_em->em_expr,
                                         const_em->em_jdomain->jd_relids,
                                         ec->ec_min_security, cur_em->em_is_const);

        // Store for selectivity estimation if it's a valid mergejoinable clause
        if (rinfo && rinfo->mergeopfamilies)
        {
            rinfo->left_ec = rinfo->right_ec = ec;
            rinfo->left_em = cur_em;
            rinfo->right_em = const_em;
            ec->ec_derives = lappend(ec->ec_derives, rinfo);
        }
    }
}
```