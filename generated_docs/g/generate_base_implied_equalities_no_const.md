# generate_base_implied_equalities_no_const

## Location
[src/backend/optimizer/path/equivclass.c:1203-1312](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/path/equivclass.c#L1203-L1312)

## Overview
Generates implied equality clauses for equivalence classes containing no pseudoconstants by creating "member1 = member2" restrictions between members of the same base relation.

## Definition

```c
structures.  Multi-relation
 * clauses will be regurgitated later by generate_join_implied_equalities().
 * (We do it this way to maintain continuity with the case that ec_broken
 * becomes set only after we've gone up a join level or two.)  However, for
 * an EC that contains constants, we can adopt a simpler strategy and just
 * throw back all the source RestrictInfos immediately;
```
## Detailed Description
This function handles equivalence classes that contain only variable members (no constants or pseudoconstants). It implements a scanning strategy that tracks the last-seen member for each base relation and generates equality clauses between consecutive members of the same relation, producing the minimum number of derived clauses needed to maintain equivalence constraints.

The algorithm scans EC members once, maintaining an array of previous members indexed by relation ID. When encountering another member from the same base relation, it generates a "prev_em = cur_em" equality clause. This approach minimizes the number of generated clauses while establishing the base case for recursive constraint propagation.

Additionally, the function ensures that all variables used in member clauses will be available at any join node by adding them to the targetlist for all relations in the equivalence class, maintaining accessibility for future join operations.

## Parameters / Member Variables
- : PlannerInfo structure containing planner state and relation information
- : EquivalenceClass containing only non-constant members to process

## Dependencies
- Functions called/Symbols referenced:
  - [bms_get_singleton_member](../b/bms_get_singleton_member.md)
  - [select_equality_operator](../s/select_equality_operator.md)
  - [process_implied_equality](../p/process_implied_equality.md)
  - [pull_var_clause](../p/pull_var_clause.md)
  - [add_vars_to_targetlist](../a/add_vars_to_targetlist.md)
  - [list_free](../l/list_free.md)
- Called from (representative examples):
  - [generate_base_implied_equalities](generate_base_implied_equalities.md)

## Notes and Other Information
- Uses an array prev_ems to track the last-seen member for each base relation
- Generates minimum number of clauses but may fail when different orderings would succeed
- Comments suggest potential improvement using UNION-FIND algorithm similar to EC merging
- Only processes members that belong to a single base relation (singleton membership)
- Marks EC as broken (ec_broken = true) if required equality operators are unavailable
- Does not add generated clauses to ec_derives to avoid cluttering with non-join clauses
- Sets mergejoinable clause markings (left_ec, right_ec, left_em, right_em) for viable clauses
- Ensures variable availability by adding all member variables to targetlists across ec_relids
- Uses PVC_RECURSE_AGGREGATES, PVC_RECURSE_WINDOWFUNCS, and PVC_INCLUDE_PLACEHOLDERS flags
- Located in src/backend/optimizer/path/equivclass.c:1203-1312

## Simplified Source

```c
static void
generate_base_implied_equalities_no_const(PlannerInfo *root, EquivalenceClass *ec)
{
    EquivalenceMember **prev_ems;
    ListCell *lc;

    // Track last-seen member for each base relation
    prev_ems = (EquivalenceMember **)
        palloc0(root->simple_rel_array_size * sizeof(EquivalenceMember *));

    // Scan members and generate equalities between consecutive members of same relation
    foreach(lc, ec->ec_members)
    {
        EquivalenceMember *cur_em = (EquivalenceMember *) lfirst(lc);
        int relid;

        // Skip multi-relation members
        if (!bms_get_singleton_member(cur_em->em_relids, &relid))
            continue;

        if (prev_ems[relid] != NULL)
        {
            EquivalenceMember *prev_em = prev_ems[relid];
            Oid eq_op;
            RestrictInfo *rinfo;

            // Find equality operator for prev_em = cur_em
            eq_op = select_equality_operator(ec, prev_em->em_datatype, cur_em->em_datatype);
            if (!OidIsValid(eq_op))
            {
                ec->ec_broken = true;
                break;
            }

            // Create the implied equality clause
            rinfo = process_implied_equality(root, eq_op, ec->ec_collation,
                                             prev_em->em_expr, cur_em->em_expr,
                                             cur_em->em_relids, ec->ec_min_security, false);

            // Mark as mergejoinable if successful
            if (rinfo && rinfo->mergeopfamilies)
            {
                rinfo->left_ec = rinfo->right_ec = ec;
                rinfo->left_em = prev_em;
                rinfo->right_em = cur_em;
            }
        }
        prev_ems[relid] = cur_em;
    }

    pfree(prev_ems);

    // Ensure all member variables are available at join nodes
    foreach(lc, ec->ec_members)
    {
        EquivalenceMember *cur_em = (EquivalenceMember *) lfirst(lc);
        List *vars = pull_var_clause((Node *) cur_em->em_expr,
                                     PVC_RECURSE_AGGREGATES |
                                     PVC_RECURSE_WINDOWFUNCS |
                                     PVC_INCLUDE_PLACEHOLDERS);

        add_vars_to_targetlist(root, vars, ec->ec_relids);
        list_free(vars);
    }
}
```