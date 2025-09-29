# ApplyRetrieveRule

## Location
[src/backend/rewrite/rewriteHandler.c:1701-1880](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L1701-L1880)

## Overview
Expands an ON SELECT rule (view definition) by converting the view's RTE to a subquery RTE containing the view's underlying query.

## Definition

```c
static Query *
ApplyRetrieveRule(Query *parsetree,
				  RewriteRule *rule,
				  int rt_index,
				  Relation relation,
				  List *activeRIRs)
```
## Detailed Description
This function implements view expansion by taking an ON SELECT rule and transforming the query to use the view's definition as a subquery. The process involves several sophisticated steps:

1. **View access restrictions**: Checks if non-system view access is restricted and enforces the restriction
2. **Result relation handling**: For views as result relations (UPDATE/DELETE/MERGE), creates a copy of the RTE to serve as the target while expanding the original for source data
3. **RETURNING clause adjustment**: Modifies RETURNING list variables to reference the new result relation for NEW values
4. **Whole-row variable addition**: Adds a resjunk whole-row variable for INSTEAD OF triggers to access OLD values
5. **Lock management**: Handles FOR UPDATE/SHARE clauses by propagating them to the view's underlying tables
6. **Recursive expansion**: Recursively expands any nested view references within the view
7. **Column count adjustment**: Handles CREATE OR REPLACE VIEW scenarios where column counts may have changed

## Parameters / Member Variables
- : The query being rewritten that references the view
- : The ON SELECT rule defining the view
- : The range table index of the view being expanded
- : The view relation being expanded
- : List of active Rules in Rangetable (for recursion detection)

## Dependencies
- Functions called/Symbols referenced:
  - copyObject
  - rt_fetch
  - [ChangeVarNodes](../C/ChangeVarNodes.md)
  - [makeWholeRowVar](../m/makeWholeRowVar.md)
  - [makeTargetEntry](../m/makeTargetEntry.md)
  - [get_parse_rowmark](../g/get_parse_rowmark.md)
  - [AcquireRewriteLocks](AcquireRewriteLocks.md)
  - [markQueryForLocking](../m/markQueryForLocking.md)
  - [fireRIRrules](../f/fireRIRrules.md)
  - RelationIsSecurityView
  - [ExecCleanTargetListLength](../E/ExecCleanTargetListLength.md)
  - [makeString](../m/makeString.md)
- Called from:
  - [fireRIRrules](../f/fireRIRrules.md)

## Notes and Other Information
- Only handles single-action ON SELECT rules without qualifications
- Supports INSTEAD OF triggers for views used as result relations in UPDATE/DELETE/MERGE
- Preserves view relation metadata (relid, relkind, etc.) for permission checking and locking
- Handles security barrier views by setting the security_barrier flag
- Creates dummy column names ("?column?") when view definitions are expanded with new columns
- Propagates row security flags from the view query to the parent query
- For INSERT operations on views as result relations, returns the query unchanged to rely on INSTEAD OF triggers

## Simplified Source

```c
static Query *
ApplyRetrieveRule(Query *parsetree,
                  RewriteRule *rule,
                  int rt_index,
                  Relation relation,
                  List *activeRIRs)
{
    Query      *rule_action;
    RangeTblEntry *rte;
    RowMarkClause *rc;
    int        numCols;

    // Basic validation
    if (list_length(rule->actions) != 1)
        elog(ERROR, "expected just one rule action");
    if (rule->qual != NULL)
        elog(ERROR, "cannot handle qualified ON SELECT rule");

    // Check view access restrictions
    if (unlikely((restrict_nonsystem_relation_kind & RESTRICT_RELKIND_VIEW) != 0 &&
                 RelationGetRelid(relation) >= FirstNormalObjectId))
        ereport(ERROR,
                (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                 errmsg("access to non-system view \"%s\" is restricted",
                        RelationGetRelationName(relation))));

    // Handle view as result relation
    if (rt_index == parsetree->resultRelation)
    {
        if (parsetree->commandType == CMD_INSERT)
            return parsetree;  // Let INSTEAD OF triggers handle it
        else if (parsetree->commandType == CMD_UPDATE ||
                 parsetree->commandType == CMD_DELETE ||
                 parsetree->commandType == CMD_MERGE)
        {
            // Create copy of RTE for target relation
            rte = rt_fetch(rt_index, parsetree->rtable);
            RangeTblEntry *newrte = copyObject(rte);
            parsetree->rtable = lappend(parsetree->rtable, newrte);
            parsetree->resultRelation = list_length(parsetree->rtable);

            // Update RETURNING list to reference new RTE
            parsetree->returningList = copyObject(parsetree->returningList);
            ChangeVarNodes((Node *) parsetree->returningList, rt_index,
                           parsetree->resultRelation, 0);

            // Add whole-row var for INSTEAD OF triggers
            Var *var = makeWholeRowVar(rte, rt_index, 0, false);
            TargetEntry *tle = makeTargetEntry((Expr *) var,
                                               list_length(parsetree->targetList) + 1,
                                               pstrdup("wholerow"),
                                               true);
            parsetree->targetList = lappend(parsetree->targetList, tle);
        }
    }

    // Handle FOR UPDATE/SHARE clauses
    rc = get_parse_rowmark(parsetree, rt_index);

    // Get view query and acquire locks
    rule_action = copyObject(linitial(rule->actions));
    AcquireRewriteLocks(rule_action, true, (rc != NULL));

    // Mark tables for locking if needed
    if (rc != NULL)
        markQueryForLocking(rule_action, (Node *) rule_action->jointree,
                            rc->strength, rc->waitPolicy, true);

    // Recursively expand nested views
    rule_action = fireRIRrules(rule_action, activeRIRs);

    // Propagate row security
    parsetree->hasRowSecurity |= rule_action->hasRowSecurity;

    // Convert RTE to subquery
    rte = rt_fetch(rt_index, parsetree->rtable);
    rte->rtekind = RTE_SUBQUERY;
    rte->subquery = rule_action;
    rte->security_barrier = RelationIsSecurityView(relation);

    // Clear inappropriate fields
    rte->tablesample = NULL;
    rte->inh = false;

    // Adjust column names if needed
    numCols = ExecCleanTargetListLength(rule_action->targetList);
    while (list_length(rte->eref->colnames) < numCols)
    {
        rte->eref->colnames = lappend(rte->eref->colnames,
                                      makeString(pstrdup("?column?")));
    }

    return parsetree;
}
```