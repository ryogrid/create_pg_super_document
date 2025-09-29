# rewriteTargetView

## Location
[src/backend/rewrite/rewriteHandler.c:3204-3864](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L3204-L3864)

## Overview
Attempts to rewrite a query where the target relation is a view, transforming it so that the view's base relation becomes the target relation.

## Definition
```c
static Query *rewriteTargetView(Query *parsetree, Relation view)
```

## Detailed Description
This function is responsible for the complex process of rewriting DML operations (INSERT, UPDATE, DELETE, MERGE) that target views to instead target the underlying base relations. This transformation is a key part of PostgreSQL's view updatability mechanism for automatically updatable views.

The function performs extensive validation to ensure the view is updatable, handles permission checking with proper user context (view owner vs. query caller based on security_invoker), adjusts column references and permissions, and handles special cases like ON CONFLICT clauses and WITH CHECK OPTION constraints.

Key operations include:
- Validating view updatability using view_query_is_auto_updatable()
- Checking that modified columns are updatable
- Creating new RTEs for the base relation with proper locking
- Adjusting variable references and permission sets
- Handling security barriers and WITH CHECK OPTION constraints
- Managing MERGE command specifics and INSTEAD OF trigger interactions

## Parameters / Member Variables
- `parsetree`: The Query node representing the DML statement targeting the view
- `view`: The Relation representing the view being targeted for modification

## Dependencies
- Functions called/Symbols referenced:
  - [get_view_query](../g/get_view_query.md)
  - copyObject
  - rt_fetch
  - [getRTEPermissionInfo](../g/getRTEPermissionInfo.md)
  - [view_query_is_auto_updatable](../v/view_query_is_auto_updatable.md)
  - [error_view_not_updatable](../e/error_view_not_updatable.md)
  - [view_cols_are_auto_updatable](../v/view_cols_are_auto_updatable.md)
  - [view_has_instead_trigger](../v/view_has_instead_trigger.md)
  - [table_open](../t/table_open.md)
  - [acquireLocksOnSubLinks](../a/acquireLocksOnSubLinks.md)
  - [ChangeVarNodes](../C/ChangeVarNodes.md)
  - [ReplaceVarsFromTargetList](../R/ReplaceVarsFromTargetList.md)
  - [addRTEPermissionInfo](../a/addRTEPermissionInfo.md)
  - [adjust_view_column_set](../a/adjust_view_column_set.md)
  - RelationHasSecurityInvoker
  - RelationIsSecurityView
  - RelationHasCheckOption
  - [AddQual](../A/AddQual.md)
- Called from (representative examples):
  - [RewriteQuery](../R/RewriteQuery.md) (src/backend/rewrite/rewriteHandler.c:4213)

## Notes and Other Information
- Requires RowExclusiveLock on the base relation since it becomes the target
- Handles security invoker vs definer semantics for permission checking
- For MERGE commands, validates that there are no partial INSTEAD OF triggers (either all actions must have triggers or none)
- Preserves security barrier semantics by moving security quals to the new target RTE
- Updates column permission bitmaps to reflect the transformation from view columns to base relation columns
- Handles special ON CONFLICT processing by creating new EXCLUDED pseudo-relations
- Implements WITH CHECK OPTION inheritance including cascaded check options
- The function assumes the view contains exactly one base relation (validated by view_query_is_auto_updatable)
- Error handling includes specific messages for different command types and non-updatable column scenarios

## Simplified Source

```c
// Simplified version of rewriteTargetView
static Query *
rewriteTargetView(Query *parsetree, Relation view)
{
    Query *viewquery;
    RangeTblEntry *base_rte, *view_rte, *new_rte;
    RTEPermissionInfo *base_perminfo, *view_perminfo, *new_perminfo;
    Relation base_rel;
    List *view_targetlist;
    int base_rt_index, new_rt_index;
    bool insert_or_update;

    // Get the view's underlying query definition
    viewquery = copyObject(get_view_query(view));

    // Find the view RTE in the outer query
    view_rte = rt_fetch(parsetree->resultRelation, parsetree->rtable);
    view_perminfo = getRTEPermissionInfo(parsetree->rteperminfos, view_rte);

    // Check if this is an INSERT or UPDATE operation
    insert_or_update = (parsetree->commandType == CMD_INSERT ||
                       parsetree->commandType == CMD_UPDATE);

    // For MERGE, check if any actions are INSERT/UPDATE
    if (parsetree->commandType == CMD_MERGE) {
        foreach_node(MergeAction, action, parsetree->mergeActionList) {
            if (action->commandType == CMD_INSERT || action->commandType == CMD_UPDATE) {
                insert_or_update = true;
                break;
            }
        }
    }

    // Validate that the view is updatable
    const char *auto_update_detail = view_query_is_auto_updatable(viewquery, insert_or_update);
    if (auto_update_detail) {
        error_view_not_updatable(view, parsetree->commandType,
                                parsetree->mergeActionList, auto_update_detail);
    }

    // For INSERT/UPDATE, validate that modified columns are updatable
    if (insert_or_update) {
        Bitmapset *modified_cols = collect_modified_columns(parsetree, view_perminfo);
        auto_update_detail = view_cols_are_auto_updatable(viewquery, modified_cols,
                                                         NULL, &non_updatable_col);
        if (auto_update_detail) {
            report_column_not_updatable_error(parsetree->commandType, non_updatable_col, view);
        }
    }

    // Get the base relation from the view query (view must contain exactly one base relation)
    RangeTblRef *rtr = linitial_node(RangeTblRef, viewquery->jointree->fromlist);
    base_rt_index = rtr->rtindex;
    base_rte = rt_fetch(base_rt_index, viewquery->rtable);
    base_perminfo = getRTEPermissionInfo(viewquery->rteperminfos, base_rte);

    // Lock the base relation (RowExclusiveLock since it becomes the target)
    base_rel = table_open(base_rte->relid, RowExclusiveLock);
    base_rte->relkind = base_rel->rd_rel->relkind;

    // Handle sublinks in the view query
    if (viewquery->hasSubLinks) {
        acquireLocksOnSubLinks_context context;
        context.for_execute = true;
        query_tree_walker(viewquery, acquireLocksOnSubLinks, &context, QTW_IGNORE_RC_SUBQUERIES);
    }

    // Create new RTE for the base relation and add to outer query
    new_rte = base_rte;
    new_rte->rellockmode = RowExclusiveLock;
    parsetree->rtable = lappend(parsetree->rtable, new_rte);
    new_rt_index = list_length(parsetree->rtable);

    // INSERTs never inherit, others use view's inheritance setting
    if (parsetree->commandType == CMD_INSERT) {
        new_rte->inh = false;
    }

    // Adjust view targetlist to reference the new base relation
    view_targetlist = viewquery->targetList;
    ChangeVarNodes((Node *) view_targetlist, base_rt_index, new_rt_index, 0);

    // Set up permissions for the new base relation RTE
    new_rte->perminfoindex = 0;
    new_perminfo = addRTEPermissionInfo(&parsetree->rteperminfos, new_rte);
    setup_base_relation_permissions(view, view_perminfo, new_perminfo, base_perminfo, view_targetlist);

    // Move security barrier quals from view to base relation
    new_rte->securityQuals = view_rte->securityQuals;
    view_rte->securityQuals = NIL;

    // Replace all references to the view with references to the base relation
    parsetree = (Query *) ReplaceVarsFromTargetList((Node *) parsetree,
                                                    parsetree->resultRelation, 0,
                                                    view_rte, view_targetlist,
                                                    REPLACEVARS_REPORT_ERROR, 0, NULL);

    // Update RTI references to point to the new base relation
    ChangeVarNodes((Node *) parsetree, parsetree->resultRelation, new_rt_index, 0);

    // Update target column numbers for INSERT/UPDATE/MERGE
    if (parsetree->commandType != CMD_DELETE) {
        update_target_column_numbers(parsetree, view_targetlist);
    }

    // Handle ON CONFLICT clauses for INSERT
    if (parsetree->onConflict && parsetree->onConflict->action == ONCONFLICT_UPDATE) {
        handle_on_conflict_rewrite(parsetree, view_rte, view_targetlist, base_rel, new_rt_index);
    }

    // Add view WHERE quals as security barriers or main WHERE clause
    if (parsetree->commandType != CMD_INSERT && viewquery->jointree->quals != NULL) {
        add_view_where_quals(parsetree, viewquery, view, base_rt_index, new_rt_index);
    }

    // Add WITH CHECK OPTION constraints for INSERT/UPDATE
    if (insert_or_update) {
        add_with_check_options(parsetree, view, viewquery, base_rt_index, new_rt_index);
    }

    table_close(base_rel, NoLock);
    return parsetree;
}
```

Key simplifications made:
- Consolidated complex validation logic into helper function calls
- Removed detailed error handling and specific error message construction
- Abstracted column permission setup and ON CONFLICT handling
- Simplified the main control flow while preserving essential algorithm steps
- Combined related operations into logical groups with descriptive comments