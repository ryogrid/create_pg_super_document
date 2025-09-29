# preprocess_targetlist

## Location
[src/backend/optimizer/prep/preptlist.c:64-347](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/optimizer/prep/preptlist.c#L64-L347)

## Overview
Driver function for preprocessing the parse tree targetlist, handling different command types (INSERT, UPDATE, DELETE, MERGE, SELECT) and preparing the targetlist for query execution.

## Definition

```c
enumber the processed_tlist entries to be consecutive.
	 */
	tlist = parse->targetList;
```
## Detailed Description
The  function is the main entry point for targetlist preprocessing in PostgreSQL's query planner. It takes a parsed query and transforms its targetlist to prepare it for execution, handling the specific requirements of different SQL command types.

For INSERT commands, it expands the targetlist to match the exact order of the target table's attributes using . For UPDATE commands, it extracts column numbers being updated via  and renumbers the processed targetlist entries to be consecutive.

The function also handles special cases like MERGE commands (which process each action's targetlist separately), adds row identity columns for UPDATE/DELETE/MERGE operations, manages junk columns for row locking (FOR UPDATE/SHARE), and processes RETURNING clauses.

The preprocessed targetlist is stored in , and for UPDATE operations, the target column numbers are stored in .

## Parameters / Member Variables
- : PlannerInfo structure containing the parsed query and planning state information

## Dependencies
- Functions called/Symbols referenced:
  - [expand_insert_targetlist](../e/expand_insert_targetlist.md)
  - [extract_update_targetlist_colnos](../e/extract_update_targetlist_colnos.md)
  - [add_row_identity_columns](../a/add_row_identity_columns.md)
  - rt_fetch
  - [table_open](../t/table_open.md)/table_close
  - [makeTargetEntry](../m/makeTargetEntry.md)
  - [makeVar](../m/makeVar.md)
  - [makeWholeRowVar](../m/makeWholeRowVar.md)
  - [pull_var_clause](pull_var_clause.md)
  - [tlist_member](../t/tlist_member.md)
  - [list_concat_copy](../l/list_concat_copy.md)
- Called from (representative examples):
  - [grouping_planner](../g/grouping_planner.md) (src/backend/optimizer/plan/planner.c:1470)

## Notes and Other Information
This function is located in src/backend/optimizer/prep/preptlist.c:64-347 and serves as a critical component in PostgreSQL's query planning phase. It must handle the complexities of different SQL command types while ensuring the targetlist is properly formatted for the executor. The function carefully manages memory and maintains proper reference relationships between query elements.

## Simplified Source

```c
void preprocess_targetlist(PlannerInfo *root)
{
    Query *parse = root->parse;
    int result_relation = parse->resultRelation;
    CmdType command_type = parse->commandType;
    RangeTblEntry *target_rte = NULL;
    Relation target_relation = NULL;
    List *tlist;

    // Open target relation if this is a modification command
    if (result_relation) {
        target_rte = rt_fetch(result_relation, parse->rtable);

        if (target_rte->rtekind != RTE_RELATION)
            elog(ERROR, "result relation must be a regular relation");

        target_relation = table_open(target_rte->relid, NoLock);
    }

    // Handle different command types
    tlist = parse->targetList;
    if (command_type == CMD_INSERT) {
        // Expand targetlist to match table attribute order
        tlist = expand_insert_targetlist(root, tlist, target_relation);
    }
    else if (command_type == CMD_UPDATE) {
        // Extract column numbers being updated
        root->update_colnos = extract_update_targetlist_colnos(tlist);
    }

    // Add row identity columns for UPDATE/DELETE/MERGE
    if ((command_type == CMD_UPDATE || command_type == CMD_DELETE ||
         command_type == CMD_MERGE) && !target_rte->inh) {
        root->processed_tlist = tlist;
        add_row_identity_columns(root, result_relation, target_rte, target_relation);
        tlist = root->processed_tlist;
    }

    // Handle MERGE command actions
    if (command_type == CMD_MERGE) {
        foreach(l, parse->mergeActionList) {
            MergeAction *action = (MergeAction *) lfirst(l);

            if (action->commandType == CMD_INSERT) {
                action->targetList = expand_insert_targetlist(root,
                                                            action->targetList,
                                                            target_relation);
            }
            else if (action->commandType == CMD_UPDATE) {
                action->updateColnos = extract_update_targetlist_colnos(action->targetList);
            }

            // Add vars from action's qual and targetlist as junk entries
            List *vars = pull_var_clause((Node *)
                                        list_concat_copy((List *) action->qual,
                                                        action->targetList),
                                        PVC_INCLUDE_PLACEHOLDERS);
            foreach(l2, vars) {
                Var *var = (Var *) lfirst(l2);
                if (IsA(var, Var) && var->varno == result_relation)
                    continue;
                if (tlist_member((Expr *) var, tlist))
                    continue;

                TargetEntry *tle = makeTargetEntry((Expr *) var,
                                                  list_length(tlist) + 1,
                                                  NULL, true);
                tlist = lappend(tlist, tle);
            }
        }

        // Add vars from merge join condition
        List *vars = pull_var_clause(parse->mergeJoinCondition, PVC_INCLUDE_PLACEHOLDERS);
        foreach(l, vars) {
            Var *var = (Var *) lfirst(l);
            if (IsA(var, Var) && var->varno == result_relation)
                continue;
            if (tlist_member((Expr *) var, tlist))
                continue;

            TargetEntry *tle = makeTargetEntry((Expr *) var,
                                              list_length(tlist) + 1,
                                              NULL, true);
            tlist = lappend(tlist, tle);
        }
    }

    // Add junk columns for row marking (FOR UPDATE/SHARE)
    foreach(lc, root->rowMarks) {
        PlanRowMark *rc = (PlanRowMark *) lfirst(lc);

        if (rc->rti != rc->prti)
            continue;

        // Add TID column if needed
        if (rc->allMarkTypes & ~(1 << ROW_MARK_COPY)) {
            Var *var = makeVar(rc->rti, SelfItemPointerAttributeNumber,
                              TIDOID, -1, InvalidOid, 0);
            char resname[32];
            snprintf(resname, sizeof(resname), "ctid%u", rc->rowmarkId);
            TargetEntry *tle = makeTargetEntry((Expr *) var,
                                              list_length(tlist) + 1,
                                              pstrdup(resname), true);
            tlist = lappend(tlist, tle);
        }

        // Add whole row if needed
        if (rc->allMarkTypes & (1 << ROW_MARK_COPY)) {
            Var *var = makeWholeRowVar(rt_fetch(rc->rti, parse->rtable),
                                      rc->rti, 0, false);
            char resname[32];
            snprintf(resname, sizeof(resname), "wholerow%u", rc->rowmarkId);
            TargetEntry *tle = makeTargetEntry((Expr *) var,
                                              list_length(tlist) + 1,
                                              pstrdup(resname), true);
            tlist = lappend(tlist, tle);
        }

        // Add tableoid for inheritance trees
        if (rc->isParent) {
            Var *var = makeVar(rc->rti, TableOidAttributeNumber,
                              OIDOID, -1, InvalidOid, 0);
            char resname[32];
            snprintf(resname, sizeof(resname), "tableoid%u", rc->rowmarkId);
            TargetEntry *tle = makeTargetEntry((Expr *) var,
                                              list_length(tlist) + 1,
                                              pstrdup(resname), true);
            tlist = lappend(tlist, tle);
        }
    }

    // Handle RETURNING clause
    if (parse->returningList && list_length(parse->rtable) > 1) {
        List *vars = pull_var_clause((Node *) parse->returningList,
                                    PVC_RECURSE_AGGREGATES |
                                    PVC_RECURSE_WINDOWFUNCS |
                                    PVC_INCLUDE_PLACEHOLDERS);
        foreach(l, vars) {
            Var *var = (Var *) lfirst(l);
            if (IsA(var, Var) && var->varno == result_relation)
                continue;
            if (tlist_member((Expr *) var, tlist))
                continue;

            TargetEntry *tle = makeTargetEntry((Expr *) var,
                                              list_length(tlist) + 1,
                                              NULL, true);
            tlist = lappend(tlist, tle);
        }
    }

    root->processed_tlist = tlist;

    if (target_relation)
        table_close(target_relation, NoLock);
}
```