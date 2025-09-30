# deconstruct_recurse

## Location
[src/backend/optimizer/plan/initsplan.c:822-1119](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/plan/initsplan.c#L822-L1119)

## Overview
Recursively traverses the query's join tree to extract join structure information and build hierarchical join lists while handling different join types and domain assignments.

## Definition

```c
static List *
deconstruct_recurse(PlannerInfo *root, Node *jtnode,
					JoinDomain *parent_domain,
					JoinTreeItem *parent_jtitem,
					List **item_list)
```
## Detailed Description
This function performs the core recursive traversal of PostgreSQL's join tree structure, processing different node types and building the necessary data structures for join planning. It handles three main types of join tree nodes:

**RangeTblRef nodes**: Base relations that are added to all_baserels and assigned to the parent domain with simple qualscope setup.

**FromExpr nodes**: Represent implicit inner joins from comma-separated table lists. The function recursively processes all child nodes and makes intelligent decisions about collapsing subproblems based on from_collapse_limit to balance planning efficiency with plan quality.

**JoinExpr nodes**: Handle explicit joins (INNER, LEFT, SEMI, ANTI, FULL) with sophisticated domain management:
- INNER/SEMI joins use the parent domain
- LEFT/ANTI joins create new child domains for proper qual placement
- FULL joins require separate domains for each side plus their own domain

The function creates JoinTreeItem structures that track essential information including qualscope (relations involved), join domains, and nonnullable_rels for outer join semantics.

## Parameters / Member Variables
- : The PlannerInfo structure containing global planning context
- : The current join tree node being processed (RangeTblRef, FromExpr, or JoinExpr)
- : The enclosing join domain for proper qual assignment
- : The parent JoinTreeItem in the hierarchy, NULL at top level
- : In/out parameter collecting JoinTreeItem structures in depth-first order

## Dependencies
- Functions called/Symbols referenced:
  - palloc0_object
  - [bms_add_member](../b/bms_add_member.md)
  - [bms_make_singleton](../b/bms_make_singleton.md)
  - [bms_add_members](../b/bms_add_members.md)
  - [bms_union](../b/bms_union.md)
  - [bms_copy](../b/bms_copy.md)
  - [list_concat](../l/list_concat.md)
  - list_make1
  - list_make2
  - llast
  - [mark_rels_nulled_by_join](../m/mark_rels_nulled_by_join.md)
  - makeNode
  - nodeTag
- Called from (representative examples):
  - [deconstruct_jointree](deconstruct_jointree.md)
  - [deconstruct_recurse](deconstruct_recurse.md) (recursive calls)

## Notes and Other Information
- Creates JoinTreeItem for each node to track structural information needed later
- Manages join domain hierarchy critical for proper qualification clause placement
- Implements join collapse logic based on from_collapse_limit and join_collapse_limit for optimization
- Handles special cases like FULL JOIN that require forced join ordering
- Tracks outer_join_rels and calls mark_rels_nulled_by_join for proper null semantics
- Different join types have varying domain assignment strategies to ensure correct qual evaluation
- The returned joinlist guides subsequent join ordering decisions in make_one_rel()
- Eliminates JOIN_RIGHT during earlier processing, handling only normalized join types

## Simplified Source

```c
static List *deconstruct_recurse(PlannerInfo *root, Node *jtnode,
                                JoinDomain *parent_domain,
                                JoinTreeItem *parent_jtitem,
                                List **item_list)
{
    List *joinlist;
    JoinTreeItem *jtitem;

    // Create JoinTreeItem for this node
    jtitem = palloc0_object(JoinTreeItem);
    jtitem->jtnode = jtnode;
    jtitem->jti_parent = parent_jtitem;

    if (IsA(jtnode, RangeTblRef)) {
        int varno = ((RangeTblRef *) jtnode)->rtindex;

        // Add base relation to global tracking
        root->all_baserels = bms_add_member(root->all_baserels, varno);

        // Assign to parent domain
        jtitem->jdomain = parent_domain;
        parent_domain->jd_relids = bms_add_member(parent_domain->jd_relids, varno);

        // Simple qualscope for single relation
        jtitem->qualscope = bms_make_singleton(varno);
        jtitem->inner_join_rels = NULL;
        joinlist = list_make1(jtnode);
    }
    else if (IsA(jtnode, FromExpr)) {
        FromExpr *f = (FromExpr *) jtnode;
        int remaining;
        ListCell *l;

        jtitem->jdomain = parent_domain;
        jtitem->qualscope = NULL;
        jtitem->inner_join_rels = NULL;
        joinlist = NIL;
        remaining = list_length(f->fromlist);

        // Process each child with collapse optimization
        foreach(l, f->fromlist) {
            JoinTreeItem *sub_item;
            List *sub_joinlist;
            int sub_members;

            sub_joinlist = deconstruct_recurse(root, lfirst(l),
                                              parent_domain, jtitem, item_list);
            sub_item = (JoinTreeItem *) llast(*item_list);

            jtitem->qualscope = bms_add_members(jtitem->qualscope,
                                               sub_item->qualscope);
            jtitem->inner_join_rels = sub_item->inner_join_rels;
            sub_members = list_length(sub_joinlist);
            remaining--;

            // Collapse subproblems if within limits
            if (sub_members <= 1 ||
                list_length(joinlist) + sub_members + remaining <= from_collapse_limit)
                joinlist = list_concat(joinlist, sub_joinlist);
            else
                joinlist = lappend(joinlist, sub_joinlist);
        }

        // Multi-element FROM is an inner join
        if (list_length(f->fromlist) > 1)
            jtitem->inner_join_rels = jtitem->qualscope;
    }
    else if (IsA(jtnode, JoinExpr)) {
        JoinExpr *j = (JoinExpr *) jtnode;
        JoinDomain *child_domain, *fj_domain;
        JoinTreeItem *left_item, *right_item;
        List *leftjoinlist, *rightjoinlist;

        switch (j->jointype) {
            case JOIN_INNER:
                // Both sides use parent domain
                jtitem->jdomain = parent_domain;
                leftjoinlist = deconstruct_recurse(root, j->larg,
                                                  parent_domain, jtitem, item_list);
                left_item = (JoinTreeItem *) llast(*item_list);
                rightjoinlist = deconstruct_recurse(root, j->rarg,
                                                   parent_domain, jtitem, item_list);
                right_item = (JoinTreeItem *) llast(*item_list);

                jtitem->qualscope = bms_union(left_item->qualscope,
                                             right_item->qualscope);
                jtitem->inner_join_rels = jtitem->qualscope;
                jtitem->left_rels = left_item->qualscope;
                jtitem->right_rels = right_item->qualscope;
                jtitem->nonnullable_rels = NULL;
                break;

            case JOIN_LEFT:
            case JOIN_ANTI:
                // Create new domain for RHS
                child_domain = makeNode(JoinDomain);
                child_domain->jd_relids = NULL;
                root->join_domains = lappend(root->join_domains, child_domain);
                jtitem->jdomain = child_domain;

                leftjoinlist = deconstruct_recurse(root, j->larg,
                                                  parent_domain, jtitem, item_list);
                left_item = (JoinTreeItem *) llast(*item_list);
                rightjoinlist = deconstruct_recurse(root, j->rarg,
                                                   child_domain, jtitem, item_list);
                right_item = (JoinTreeItem *) llast(*item_list);

                // Update domain membership
                parent_domain->jd_relids = bms_add_members(parent_domain->jd_relids,
                                                          child_domain->jd_relids);
                jtitem->qualscope = bms_union(left_item->qualscope,
                                             right_item->qualscope);

                if (j->rtindex != 0) {
                    parent_domain->jd_relids = bms_add_member(parent_domain->jd_relids,
                                                             j->rtindex);
                    jtitem->qualscope = bms_add_member(jtitem->qualscope, j->rtindex);
                    root->outer_join_rels = bms_add_member(root->outer_join_rels,
                                                          j->rtindex);
                    mark_rels_nulled_by_join(root, j->rtindex, right_item->qualscope);
                }

                jtitem->inner_join_rels = bms_union(left_item->inner_join_rels,
                                                   right_item->inner_join_rels);
                jtitem->left_rels = left_item->qualscope;
                jtitem->right_rels = right_item->qualscope;
                jtitem->nonnullable_rels = left_item->qualscope;
                break;

            case JOIN_SEMI:
                // Both sides use parent domain
                jtitem->jdomain = parent_domain;
                leftjoinlist = deconstruct_recurse(root, j->larg,
                                                  parent_domain, jtitem, item_list);
                left_item = (JoinTreeItem *) llast(*item_list);
                rightjoinlist = deconstruct_recurse(root, j->rarg,
                                                   parent_domain, jtitem, item_list);
                right_item = (JoinTreeItem *) llast(*item_list);

                jtitem->qualscope = bms_union(left_item->qualscope,
                                             right_item->qualscope);
                jtitem->inner_join_rels = bms_union(left_item->inner_join_rels,
                                                   right_item->inner_join_rels);
                jtitem->left_rels = left_item->qualscope;
                jtitem->right_rels = right_item->qualscope;
                jtitem->nonnullable_rels = NULL;
                break;

            case JOIN_FULL:
                // FULL JOIN gets its own domain, each side gets separate domains
                fj_domain = makeNode(JoinDomain);
                root->join_domains = lappend(root->join_domains, fj_domain);
                jtitem->jdomain = fj_domain;

                // Left side domain
                child_domain = makeNode(JoinDomain);
                child_domain->jd_relids = NULL;
                root->join_domains = lappend(root->join_domains, child_domain);
                leftjoinlist = deconstruct_recurse(root, j->larg,
                                                  child_domain, jtitem, item_list);
                left_item = (JoinTreeItem *) llast(*item_list);
                fj_domain->jd_relids = bms_copy(child_domain->jd_relids);

                // Right side domain
                child_domain = makeNode(JoinDomain);
                child_domain->jd_relids = NULL;
                root->join_domains = lappend(root->join_domains, child_domain);
                rightjoinlist = deconstruct_recurse(root, j->rarg,
                                                   child_domain, jtitem, item_list);
                right_item = (JoinTreeItem *) llast(*item_list);

                // Update domains and track nulling
                fj_domain->jd_relids = bms_add_members(fj_domain->jd_relids,
                                                      child_domain->jd_relids);
                parent_domain->jd_relids = bms_add_members(parent_domain->jd_relids,
                                                          fj_domain->jd_relids);
                parent_domain->jd_relids = bms_add_member(parent_domain->jd_relids,
                                                         j->rtindex);

                jtitem->qualscope = bms_union(left_item->qualscope,
                                             right_item->qualscope);
                jtitem->qualscope = bms_add_member(jtitem->qualscope, j->rtindex);
                root->outer_join_rels = bms_add_member(root->outer_join_rels,
                                                      j->rtindex);

                mark_rels_nulled_by_join(root, j->rtindex, left_item->qualscope);
                mark_rels_nulled_by_join(root, j->rtindex, right_item->qualscope);

                jtitem->inner_join_rels = bms_union(left_item->inner_join_rels,
                                                   right_item->inner_join_rels);
                jtitem->left_rels = left_item->qualscope;
                jtitem->right_rels = right_item->qualscope;
                jtitem->nonnullable_rels = jtitem->qualscope;
                break;

            default:
                elog(ERROR, \"unrecognized join type: %d\", (int) j->jointype);
                break;
        }

        // Compute output joinlist with collapse logic
        if (j->jointype == JOIN_FULL) {
            joinlist = list_make1(list_make2(leftjoinlist, rightjoinlist));
        } else if (list_length(leftjoinlist) + list_length(rightjoinlist) <=
                   join_collapse_limit) {
            joinlist = list_concat(leftjoinlist, rightjoinlist);
        } else {
            Node *leftpart = (list_length(leftjoinlist) == 1) ?
                            (Node *) linitial(leftjoinlist) : (Node *) leftjoinlist;
            Node *rightpart = (list_length(rightjoinlist) == 1) ?
                             (Node *) linitial(rightjoinlist) : (Node *) rightjoinlist;
            joinlist = list_make2(leftpart, rightpart);
        }
    }
    else {
        elog(ERROR, \"unrecognized node type: %d\", (int) nodeTag(jtnode));
        joinlist = NIL;
    }

    // Add completed JoinTreeItem to list
    *item_list = lappend(*item_list, jtitem);
    return joinlist;
}
```