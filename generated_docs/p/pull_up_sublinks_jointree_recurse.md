# pull_up_sublinks_jointree_recurse

## Location
[src/backend/optimizer/prep/prepjointree.c:480-636](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L480-L636)

## Overview
Recursively processes jointree nodes for pull_up_sublinks, transforming SubLinks into semijoins while collecting relids of contained relations.

## Definition

```c
union(leftrelids,
																   rightrelids),
														 NULL, NULL);
```
## Detailed Description
This is the core recursive function that implements the SubLink pull-up transformation for different types of jointree nodes. It traverses the query's jointree structure and delegates SubLink processing to pull_up_sublinks_qual_recurse for the qualification clauses.

The function handles different jointree node types:

**RangeTblRef**: Simple base case that returns the relid of the referenced relation without modification.

**FromExpr**: Processes each child in the fromlist recursively, then processes the WHERE clause qualifications. The function builds a new FromExpr with the transformed children and calls pull_up_sublinks_qual_recurse to handle SubLinks in the quals.

**JoinExpr**: Creates a copy of the join node and recursively processes both left and right arguments. The handling of the join quals depends on the join type:
- **INNER JOIN**: SubLinks can be pulled up freely since all relations are available
- **LEFT JOIN**: SubLinks can only be pulled up if they reference the nullable (right) side  
- **RIGHT JOIN**: SubLinks can only be pulled up if they reference the nullable (left) side
- **FULL JOIN**: No SubLink pull-up is performed since both sides may be nullable

The function ensures that pulled-up SubLinks are placed correctly in the join tree structure and maintains proper relid tracking for subsequent optimization phases.

## Parameters / Member Variables
- : PlannerInfo structure containing query optimization context
- : The jointree node to process (RangeTblRef, FromExpr, or JoinExpr)
- : Output parameter that receives the set of relation IDs contained in this subtree

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md)
  - [pull_up_sublinks_qual_recurse](pull_up_sublinks_qual_recurse.md)
  - [makeFromExpr](../m/makeFromExpr.md)
  - [bms_make_singleton](../b/bms_make_singleton.md), bms_join, bms_union, bms_add_member
  - [palloc](palloc.md), memcpy
  - [lappend](../l/lappend.md), lfirst
  - IsA macro
  - elog, nodeTag
  - JOIN_INNER, JOIN_LEFT, JOIN_RIGHT, JOIN_FULL constants
- Called from (representative examples):
  - [pull_up_sublinks](pull_up_sublinks.md) (in src/backend/optimizer/prep/prepjointree.c:459)  
  - [pull_up_sublinks_qual_recurse](pull_up_sublinks_qual_recurse.md) (multiple locations for recursive SubLink processing)
  - Self-recursive calls for processing child nodes

## Notes and Other Information
- Stack overflow protection via check_stack_depth() due to recursive nature
- Creates modified copies of JoinExpr nodes using palloc/memcpy to avoid affecting original tree
- Handles join alias variables correctly by including join rtindex in returned relids
- Does not include pulled-up subquery relids in returned relids since upper levels cannot reference them
- Relies on subsequent optimization steps to flatten and rearrange the resulting join structure
- Critical component of subquery decorrelation and semijoin optimization
- Works in close coordination with pull_up_sublinks_qual_recurse for actual SubLink transformation

## Simplified Source

```c
static Node *
pull_up_sublinks_jointree_recurse(PlannerInfo *root, Node *jtnode, Relids *relids)
{
    check_stack_depth();  // Prevent stack overflow

    if (jtnode == NULL) {
        *relids = NULL;
    }
    else if (IsA(jtnode, RangeTblRef)) {
        // Base relation - return relid, no modification needed
        int varno = ((RangeTblRef *) jtnode)->rtindex;
        *relids = bms_make_singleton(varno);
    }
    else if (IsA(jtnode, FromExpr)) {
        // FROM expression - process children and qualifications
        FromExpr *f = (FromExpr *) jtnode;
        List *newfromlist = NIL;
        Relids frelids = NULL;

        // Process each child recursively
        foreach(l, f->fromlist) {
            Node *newchild;
            Relids childrelids;

            newchild = pull_up_sublinks_jointree_recurse(root, lfirst(l), &childrelids);
            newfromlist = lappend(newfromlist, newchild);
            frelids = bms_join(frelids, childrelids);
        }

        // Build new FromExpr and process qualifications
        FromExpr *newf = makeFromExpr(newfromlist, NULL);
        Node *jtlink = (Node *) newf;

        newf->quals = pull_up_sublinks_qual_recurse(root, f->quals,
                                                   &jtlink, frelids,
                                                   NULL, NULL);

        *relids = frelids;
        jtnode = jtlink;
    }
    else if (IsA(jtnode, JoinExpr)) {
        // Join expression - handle based on join type
        JoinExpr *j = (JoinExpr *) palloc(sizeof(JoinExpr));
        memcpy(j, jtnode, sizeof(JoinExpr));
        Node *jtlink = (Node *) j;
        Relids leftrelids, rightrelids;

        // Process left and right arguments
        j->larg = pull_up_sublinks_jointree_recurse(root, j->larg, &leftrelids);
        j->rarg = pull_up_sublinks_jointree_recurse(root, j->rarg, &rightrelids);

        // Process qualifications based on join type
        switch (j->jointype) {
            case JOIN_INNER:
                j->quals = pull_up_sublinks_qual_recurse(root, j->quals, &jtlink,
                                                        bms_union(leftrelids, rightrelids),
                                                        NULL, NULL);
                break;
            case JOIN_LEFT:
                j->quals = pull_up_sublinks_qual_recurse(root, j->quals, &j->rarg,
                                                        rightrelids, NULL, NULL);
                break;
            case JOIN_RIGHT:
                j->quals = pull_up_sublinks_qual_recurse(root, j->quals, &j->larg,
                                                        leftrelids, NULL, NULL);
                break;
            case JOIN_FULL:
                // Cannot pull up SubLinks in full joins
                break;
            default:
                elog(ERROR, "unrecognized join type: %d", (int) j->jointype);
        }

        *relids = bms_join(leftrelids, rightrelids);
        if (j->rtindex)
            *relids = bms_add_member(*relids, j->rtindex);
        jtnode = jtlink;
    }
    else
        elog(ERROR, "unrecognized node type: %d", (int) nodeTag(jtnode));

    return jtnode;
}
```