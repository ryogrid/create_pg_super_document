# rewriteRuleAction

## Location
[src/backend/rewrite/rewriteHandler.c:349-701](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L349-L701)

## Overview
Rewrites a rule action query by merging it with the triggering query, applying appropriate qualifiers, and adjusting variable references and range table entries.

## Definition
```c
static Query *rewriteRuleAction(Query *parsetree, Query *rule_action, Node *rule_qual, int rt_index, CmdType event, bool *returning_flag)
```

## Detailed Description
rewriteRuleAction is a core function in PostgreSQL's rule rewriting system that transforms a rule action query to incorporate context from the triggering query. This complex process involves multiple phases:

1. **Lock Acquisition**: Acquires necessary locks on all relations referenced in the rule action and qualifiers
2. **Variable Node Offsetting**: Adjusts variable reference numbers (varnos) to accommodate the merged range tables
3. **Range Table Merging**: Combines the original query's range table with the rule action's range table 
4. **LATERAL Marking**: Marks subquery RTEs as LATERAL if they contain references to the current query level
5. **Join Tree Adjustment**: Merges join trees from both queries while handling duplicate references
6. **CTE Handling**: Merges Common Table Expressions while checking for name conflicts
7. **Qualifier Addition**: Adds rule qualifiers and original query qualifiers to the rule action
8. **Target List Rewriting**: Replaces NEW.attribute references with actual target list entries
9. **RETURNING Clause Processing**: Handles RETURNING clauses from both the rule and triggering query

The function ensures proper isolation and variable scoping while maintaining semantic correctness of the rewritten query.

## Parameters / Member Variables
- `parsetree`: The original triggering query
- `rule_action`: The rule action query to be rewritten
- `rule_qual`: WHERE condition of the rule (NULL if unconditional)
- `rt_index`: Range table index of the result relation in the original query
- `event`: Type of rule event (INSERT, UPDATE, DELETE)
- `returning_flag`: Output flag set to true if RETURNING clause is rewritten (must be initialized to false)

## Dependencies
- Functions called/Symbols referenced:
  - [AcquireRewriteLocks](../A/AcquireRewriteLocks.md)
  - [acquireLocksOnSubLinks](../a/acquireLocksOnSubLinks.md)
  - copyObject
  - [getInsertSelectQuery](../g/getInsertSelectQuery.md)
  - [OffsetVarNodes](../O/OffsetVarNodes.md)
  - [ChangeVarNodes](../C/ChangeVarNodes.md)
  - [adjustJoinTreeList](../a/adjustJoinTreeList.md)
  - [CombineRangeTables](../C/CombineRangeTables.md)
  - [contain_vars_of_level](../c/contain_vars_of_level.md)
  - [rangeTableEntry_used](rangeTableEntry_used.md)
  - [checkExprHasSubLink](../c/checkExprHasSubLink.md)
  - [ReplaceVarsFromTargetList](../R/ReplaceVarsFromTargetList.md)
  - [AddQual](../A/AddQual.md)
  - rt_fetch
- Called from (representative examples):
  - [fireRules](../f/fireRules.md)

## Notes and Other Information
- This is a static function, only accessible within rewriteHandler.c
- Creates modifiable copies of input queries to avoid modifying cached versions
- Handles complex edge cases like INSERT...SELECT rules and set operations
- Enforces restrictions such as disallowing conditional UNION/INTERSECT/EXCEPT statements
- Prevents name conflicts between CTEs in original and rule action queries
- Manages proper variable scoping between OLD/NEW references and actual table references
- Critical for PostgreSQL's rule system which enables views, triggers, and other rewrite-based features
- The function maintains hasSubLinks, hasRowSecurity, hasRecursive, and hasModifyingCTE flags correctly across the merge

## Simplified Source

```c
static Query *
rewriteRuleAction(Query *parsetree,
                  Query *rule_action,
                  Node *rule_qual,
                  int rt_index,
                  CmdType event,
                  bool *returning_flag)
{
    int current_varno, new_varno, rt_length;
    Query *sub_action;

    // Create safe copies to avoid modifying cached data
    rule_action = copyObject(rule_action);
    rule_qual = copyObject(rule_qual);

    // Acquire locks on all referenced relations
    AcquireRewriteLocks(rule_action, true, false);
    acquireLocksOnSubLinks_context context;
    context.for_execute = true;
    (void) acquireLocksOnSubLinks(rule_qual, &context);

    // Calculate variable offsets for merging range tables
    current_varno = rt_index;
    rt_length = list_length(parsetree->rtable);
    new_varno = PRS2_NEW_VARNO + rt_length;

    // Handle INSERT...SELECT special case
    sub_action = getInsertSelectQuery(rule_action, &sub_action_ptr);

    // Adjust variable references for merged range table
    OffsetVarNodes((Node *) sub_action, rt_length, 0);
    OffsetVarNodes(rule_qual, rt_length, 0);

    // Map OLD references to the original relation
    ChangeVarNodes((Node *) sub_action, PRS2_OLD_VARNO + rt_length, rt_index, 0);
    ChangeVarNodes(rule_qual, PRS2_OLD_VARNO + rt_length, rt_index, 0);

    // Mark subquery RTEs as LATERAL if they reference NEW/OLD
    foreach(lc, sub_action->rtable) {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
        if (rte->rtekind == RTE_SUBQUERY && !rte->lateral &&
            contain_vars_of_level((Node *) rte->subquery, 1))
            rte->lateral = true;
    }

    // Merge range tables from original and rule queries
    List *rtable_tail = sub_action->rtable;
    List *perminfos_tail = sub_action->rteperminfos;

    sub_action->rtable = copyObject(parsetree->rtable);
    sub_action->rteperminfos = copyObject(parsetree->rteperminfos);
    CombineRangeTables(&sub_action->rtable, &sub_action->rteperminfos,
                       rtable_tail, perminfos_tail);

    // Merge join trees if not a utility command
    if (sub_action->commandType != CMD_UTILITY) {
        bool keeporig = (!rangeTableEntry_used((Node *) sub_action->jointree, rt_index, 0)) &&
                       (rangeTableEntry_used(rule_qual, rt_index, 0) ||
                        rangeTableEntry_used(parsetree->jointree->quals, rt_index, 0));

        List *newjointree = adjustJoinTreeList(parsetree, !keeporig, rt_index);
        if (newjointree != NIL) {
            sub_action->jointree->fromlist =
                list_concat(newjointree, sub_action->jointree->fromlist);
        }
    }

    // Merge CTEs if present
    if (parsetree->cteList != NIL && sub_action->commandType != CMD_UTILITY) {
        sub_action->cteList = list_concat(sub_action->cteList,
                                          copyObject(parsetree->cteList));
        sub_action->hasRecursive |= parsetree->hasRecursive;
        sub_action->hasModifyingCTE |= parsetree->hasModifyingCTE;
    }

    // Add qualifications from rule and original query
    AddQual(sub_action, rule_qual);
    AddQual(sub_action, parsetree->jointree->quals);

    // Replace NEW references with target list values for INSERT/UPDATE
    if ((event == CMD_INSERT || event == CMD_UPDATE) &&
        sub_action->commandType != CMD_UTILITY) {
        sub_action = (Query *)
            ReplaceVarsFromTargetList((Node *) sub_action,
                                      new_varno, 0,
                                      rt_fetch(new_varno, sub_action->rtable),
                                      parsetree->targetList,
                                      (event == CMD_UPDATE) ?
                                      REPLACEVARS_CHANGE_VARNO :
                                      REPLACEVARS_SUBSTITUTE_NULL,
                                      current_varno, NULL);
    }

    // Handle RETURNING clause
    if (!parsetree->returningList) {
        rule_action->returningList = NIL;
    } else if (rule_action->returningList) {
        if (*returning_flag)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("cannot have RETURNING lists in multiple rules")));
        *returning_flag = true;
        rule_action->returningList = (List *)
            ReplaceVarsFromTargetList((Node *) parsetree->returningList,
                                      parsetree->resultRelation, 0,
                                      rt_fetch(parsetree->resultRelation, parsetree->rtable),
                                      rule_action->returningList,
                                      REPLACEVARS_REPORT_ERROR, 0,
                                      &rule_action->hasSubLinks);
    }

    return rule_action;
}
```