# transform_MERGE_to_join

## Location
[src/backend/optimizer/prep/prepjointree.c:168-394](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/prepjointree.c#L168-L394)

## Overview
Transforms a MERGE statement's jointree to include the target relation by creating an appropriate join between the source and target relations.

## Definition

```c
void
transform_MERGE_to_join(Query *parse)
```
## Detailed Description
This function is responsible for converting a MERGE statement into a join operation that includes both the target and source relations. It analyzes the MERGE action list to determine the appropriate join type (INNER, LEFT, RIGHT, or FULL) based on the presence of different WHEN clauses:

- **INNER JOIN**: Used when only WHEN MATCHED actions exist
- **LEFT JOIN**: Used when WHEN NOT MATCHED BY SOURCE actions exist  
- **RIGHT JOIN**: Used when WHEN NOT MATCHED BY TARGET actions exist
- **FULL JOIN**: Used when both NOT MATCHED BY SOURCE and NOT MATCHED BY TARGET actions exist

The function creates a new RangeTblEntry for the join, constructs a JoinExpr that combines the target and source relations, and updates the query's jointree accordingly. It also handles nullability adjustments for variables that may be affected by the outer join conditions.

## Parameters / Member Variables
- `*parse`: The Query structure representing the MERGE statement to be transformed
## Dependencies
- Functions called/Symbols referenced:
  - makeNode (for creating RangeTblEntry, JoinExpr, RangeTblRef, FromExpr, NullTest)
  - [makeAlias](../m/makeAlias.md)
  - [makeFromExpr](../m/makeFromExpr.md)  
  - [makeWholeRowVar](../m/makeWholeRowVar.md)
  - [add_nulling_relids](../a/add_nulling_relids.md)
  - [bms_make_singleton](../b/bms_make_singleton.md)
  - rt_fetch
  - [make_and_qual](../m/make_and_qual.md)
  - foreach_node macro
  - [lappend](../l/lappend.md), linitial, list_make1, list_length
  - IsA macro
  - elog, Assert
- Called from (representative examples):
  - [subquery_planner](../s/subquery_planner.md) (in src/backend/optimizer/plan/planner.c:700)

## Notes and Other Information
- Only processes queries with commandType == CMD_MERGE, returns early otherwise
- Creates a synthetic join RTE with eref alias "*MERGE*"
- For trigger-updatable views, handles the expanded view subquery as the target
- Adds nulling relids to handle nullable variables in outer joins
- Optimizes by setting mergeJoinCondition to NULL when no NOT MATCHED BY SOURCE actions exist
- When NOT MATCHED BY SOURCE actions exist, adds "src IS NOT NULL" check to prevent incorrect results during recheck evaluation

## Simplified Source

```c
void transform_MERGE_to_join(Query *parse) {
    if (parse->commandType != CMD_MERGE)
        return;

    // Determine what join type is needed based on MERGE actions
    bool have_action[NUM_MERGE_MATCH_KINDS] = {false, false, false};

    foreach_node(MergeAction, action, parse->mergeActionList) {
        if (action->commandType != CMD_NOTHING)
            have_action[action->matchKind] = true;
    }

    // Choose join type based on which WHEN clauses exist
    JoinType jointype;
    if (have_action[MERGE_WHEN_NOT_MATCHED_BY_SOURCE] &&
        have_action[MERGE_WHEN_NOT_MATCHED_BY_TARGET])
        jointype = JOIN_FULL;
    else if (have_action[MERGE_WHEN_NOT_MATCHED_BY_SOURCE])
        jointype = JOIN_LEFT;
    else if (have_action[MERGE_WHEN_NOT_MATCHED_BY_TARGET])
        jointype = JOIN_RIGHT;
    else
        jointype = JOIN_INNER;

    // Create join RTE with appropriate type
    RangeTblEntry *joinrte = makeNode(RangeTblEntry);
    joinrte->rtekind = RTE_JOIN;
    joinrte->jointype = jointype;
    joinrte->eref = makeAlias("*MERGE*", NIL);

    // Add to range table and get index
    parse->rtable = lappend(parse->rtable, joinrte);
    int joinrti = list_length(parse->rtable);

    // Setup target relation (with any quals)
    RangeTblRef *target_rtr = makeNode(RangeTblRef);
    target_rtr->rtindex = parse->mergeTargetRelation;
    FromExpr *target = makeFromExpr(list_make1(target_rtr), parse->jointree->quals);

    // Get source relation
    Node *source = linitial(parse->jointree->fromlist);
    int sourcerti = IsA(source, RangeTblRef) ?
        ((RangeTblRef *) source)->rtindex :
        ((JoinExpr *) source)->rtindex;

    // Create the join expression
    JoinExpr *joinexpr = makeNode(JoinExpr);
    joinexpr->jointype = jointype;
    joinexpr->larg = (Node *) target;
    joinexpr->rarg = source;
    joinexpr->quals = parse->mergeJoinCondition;
    joinexpr->rtindex = joinrti;

    // Replace the query's fromlist with our new join
    parse->jointree->fromlist = list_make1(joinexpr);
    parse->jointree->quals = NULL;

    // Handle nullability for outer joins
    if (jointype == JOIN_LEFT || jointype == JOIN_FULL) {
        // Mark source vars as nullable in join condition and actions
        parse->mergeJoinCondition = add_nulling_relids(
            parse->mergeJoinCondition,
            bms_make_singleton(sourcerti),
            bms_make_singleton(joinrti));

        foreach_node(MergeAction, action, parse->mergeActionList) {
            action->qual = add_nulling_relids(action->qual,
                bms_make_singleton(sourcerti), bms_make_singleton(joinrti));
            action->targetList = (List *) add_nulling_relids(
                (Node *) action->targetList,
                bms_make_singleton(sourcerti), bms_make_singleton(joinrti));
        }
    }

    // Add source NULL check for NOT MATCHED BY SOURCE handling
    if (have_action[MERGE_WHEN_NOT_MATCHED_BY_SOURCE]) {
        Var *src_var = makeWholeRowVar(rt_fetch(sourcerti, parse->rtable),
                                       sourcerti, 0, false);
        src_var->varnullingrels = bms_make_singleton(joinrti);

        NullTest *ntest = makeNode(NullTest);
        ntest->arg = (Expr *) src_var;
        ntest->nulltesttype = IS_NOT_NULL;

        parse->mergeJoinCondition = (Node *) make_and_qual(
            (Node *) ntest, parse->mergeJoinCondition);
    } else {
        parse->mergeJoinCondition = NULL;  // Not needed
    }
}
```