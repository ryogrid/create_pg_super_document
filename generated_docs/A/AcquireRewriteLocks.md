# AcquireRewriteLocks

## Location
[src/backend/rewrite/rewriteHandler.c:146-307](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L146-L307)

## Overview
Acquires suitable locks on all relations mentioned in a Query to ensure that relation schemas don't change during rewriting, planning, and executing the query.

## Definition

```c
void
AcquireRewriteLocks(Query *parsetree,
					bool forExecute,
					bool forUpdatePushedDown)
```
## Detailed Description
AcquireRewriteLocks is a crucial function in PostgreSQL's query rewriting system that ensures schema stability during query processing. It traverses all Range Table Entries (RTEs) in a query tree and acquires appropriate locks on the referenced relations.

The function handles three types of RTEs:
1. **RTE_RELATION**: Acquires locks on base relations using either the specified lock mode or AccessShareLock
2. **RTE_JOIN**: Processes join alias variables and replaces references to dropped columns with NULL pointers
3. **RTE_SUBQUERY**: Recursively processes subqueries to acquire locks on their relations

The function also handles Common Table Expressions (CTEs) and sublinks, ensuring that all referenced relations are properly locked throughout the query tree hierarchy.

A key secondary function is fixing up JOIN RTE references to dropped columns by replacing join alias vars that reference dropped columns with null pointers, which supports the get_rte_attribute_is_dropped() function efficiently.

## Parameters
- : The Query tree to process for lock acquisition
- : If true, uses RTE rellockmode fields; if false, uses AccessShareLock on all relations
- : Indicates that a pushed-down FOR [KEY] UPDATE/SHARE applies, requiring at least RowShareLock on all relations

## Dependencies
- Functions called/Symbols referenced:
  - [strip_implicit_coercions](../s/strip_implicit_coercions.md)
  - rt_fetch
  - [get_rte_attribute_is_dropped](../g/get_rte_attribute_is_dropped.md)
  - [get_parse_rowmark](../g/get_parse_rowmark.md)
  - query_tree_walker
  - [acquireLocksOnSubLinks](../a/acquireLocksOnSubLinks.md)
  - [table_open](../t/table_open.md)/table_close
- Called from (representative examples):
  - [rewriteRuleAction](../r/rewriteRuleAction.md)
  - [ApplyRetrieveRule](ApplyRetrieveRule.md)
  - [refresh_matview_datafill](../r/refresh_matview_datafill.md)
  - [init_sql_fcache](../i/init_sql_fcache.md)
  - [inline_set_returning_function](../i/inline_set_returning_function.md)

## Notes and Other Information
- This function modifies the querytree in-place, so callers should typically use copyObject() first
- Lock acquisition can be skipped when the querytree was just built by the parser since parse analysis already acquired the same locks
- The function recursively calls itself for subqueries and CTEs
- Performance optimization: replaced the O(N^2) recursive approach from PostgreSQL 8.0 with direct null pointer replacement for dropped columns in JOINs
- All acquired locks are held until end of transaction to protect against schema changes during query execution

## Simplified Source

```c
void AcquireRewriteLocks(Query *parsetree, bool forExecute, bool forUpdatePushedDown) {
    acquireLocksOnSubLinks_context context;
    context.for_execute = forExecute;

    int rt_index = 0;

    // Process each Range Table Entry
    foreach(l, parsetree->rtable) {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(l);
        ++rt_index;

        switch (rte->rtekind) {
            case RTE_RELATION:
                // Determine appropriate lock mode
                LOCKMODE lockmode;
                if (!forExecute)
                    lockmode = AccessShareLock;
                else if (forUpdatePushedDown) {
                    if (rte->rellockmode == AccessShareLock)
                        rte->rellockmode = RowShareLock;
                    lockmode = rte->rellockmode;
                }
                else
                    lockmode = rte->rellockmode;

                // Open relation with lock and update RTE info
                rel = table_open(rte->relid, lockmode);
                rte->relkind = rel->rd_rel->relkind;
                table_close(rel, NoLock);
                break;

            case RTE_JOIN:
                // Fix dropped columns in join alias vars
                newaliasvars = NIL;
                foreach(ll, rte->joinaliasvars) {
                    Var *aliasitem = (Var *) lfirst(ll);
                    Var *aliasvar = (Var *) strip_implicit_coercions((Node *) aliasitem);

                    // Check if column was dropped and replace with NULL
                    if (aliasvar && IsA(aliasvar, Var)) {
                        RangeTblEntry *inputrte = rt_fetch(aliasvar->varno, parsetree->rtable);
                        if (get_rte_attribute_is_dropped(inputrte, aliasvar->varattno))
                            aliasitem = NULL;
                    }
                    newaliasvars = lappend(newaliasvars, aliasitem);
                }
                rte->joinaliasvars = newaliasvars;
                break;

            case RTE_SUBQUERY:
                // Recursively process subquery
                AcquireRewriteLocks(rte->subquery, forExecute,
                    forUpdatePushedDown || get_parse_rowmark(parsetree, rt_index) != NULL);
                break;
        }
    }

    // Process Common Table Expressions (WITH clauses)
    foreach(l, parsetree->cteList) {
        CommonTableExpr *cte = (CommonTableExpr *) lfirst(l);
        AcquireRewriteLocks((Query *) cte->ctequery, forExecute, false);
    }

    // Process sublinks if present
    if (parsetree->hasSubLinks)
        query_tree_walker(parsetree, acquireLocksOnSubLinks, &context, QTW_IGNORE_RC_SUBQUERIES);
}
```