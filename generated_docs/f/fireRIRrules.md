# fireRIRrules

## Location
[src/backend/rewrite/rewriteHandler.c:1982-2310](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/rewrite/rewriteHandler.c#L1982-L2310)

## Overview
Applies all RIR (Rules Instead Rewrite) rules on each range table entry in the given query, handling view expansion, rule recursion detection, and row-level security policies.

## Definition

```c
structuring so that
	 * we only need to process the qual this way once.)
	 */
	(void) acquireLocksOnSubLinks(new_qual, &context);
```
## Detailed Description
fireRIRrules is the core function of PostgreSQL's rule rewriting system that processes a query tree to apply RIR rules. It systematically examines each range table entry (RTE) in the query, applying appropriate SELECT rules (typically from views) while maintaining recursion detection through the activeRIRs list. The function handles multiple aspects of query rewriting including:

1. Expanding SEARCH and CYCLE clauses in CTEs (Common Table Expressions)
2. Processing subqueries recursively by calling itself
3. Applying view rules while detecting infinite recursion
4. Handling row-level security (RLS) policies
5. Processing sublinks within expressions
6. Managing security barriers and with-check options

The function modifies the parse tree in-place, potentially replacing simple table references with complex subqueries derived from view definitions or rule actions.

## Parameters / Member Variables
- : The Query structure to process and rewrite
- : List of OIDs for views currently being processed (used for recursion detection)

## Dependencies
- Functions called/Symbols referenced:
  - [rewriteSearchAndCycle](../r/rewriteSearchAndCycle.md)
  - rt_fetch
  - [rangeTableEntry_used](../r/rangeTableEntry_used.md)
  - [table_open](../t/table_open.md)
  - [ApplyRetrieveRule](../A/ApplyRetrieveRule.md)
  - [get_row_security_policies](../g/get_row_security_policies.md)
  - [acquireLocksOnSubLinks](../a/acquireLocksOnSubLinks.md)
  - [fireRIRonSubLink](fireRIRonSubLink.md)
  - query_tree_walker
  - expression_tree_walker
  - [list_member_oid](../l/list_member_oid.md)
  - [lappend_oid](../l/lappend_oid.md)
  - [list_delete_last](../l/list_delete_last.md)
  - [list_concat](../l/list_concat.md)
- Called from (representative examples):
  - [QueryRewrite](../Q/QueryRewrite.md)
  - [ApplyRetrieveRule](../A/ApplyRetrieveRule.md)
  - [fireRIRonSubLink](fireRIRonSubLink.md) (for subqueries)

## Notes and Other Information
- Central function in PostgreSQL's rule rewriting system for view expansion
- Handles complex scenarios like materialized views, EXCLUDED pseudo-relations in UPSERT
- Implements sophisticated recursion detection to prevent infinite loops
- Processes row-level security policies as a final step after rule application
- Maintains hasRowSecurity and hasSubLinks flags throughout the query tree
- Uses special handling for range table entries that are not referenced in the query
- Skips materialized views to prevent inappropriate expansion during queries

## Simplified Source

```c
static Query *
fireRIRrules(Query *parsetree, List *activeRIRs)
{
    int origResultRelation = parsetree->resultRelation;
    int rt_index;
    ListCell *lc;

    // Expand SEARCH and CYCLE clauses in CTEs
    foreach(lc, parsetree->cteList)
    {
        CommonTableExpr *cte = lfirst_node(CommonTableExpr, lc);

        if (cte->search_clause || cte->cycle_clause)
        {
            cte = rewriteSearchAndCycle(cte);
            lfirst(lc) = cte;
        }
    }

    // Process each range table entry for rule application
    rt_index = 0;
    while (rt_index < list_length(parsetree->rtable))
    {
        RangeTblEntry *rte;
        Relation rel;
        List *locks;
        RuleLock *rules;
        RewriteRule *rule;
        int i;

        ++rt_index;
        rte = rt_fetch(rt_index, parsetree->rtable);

        // Handle subqueries recursively
        if (rte->rtekind == RTE_SUBQUERY)
        {
            rte->subquery = fireRIRrules(rte->subquery, activeRIRs);
            parsetree->hasRowSecurity |= rte->subquery->hasRowSecurity;
            continue;
        }

        // Skip non-relation RTEs
        if (rte->rtekind != RTE_RELATION)
            continue;

        // Skip materialized views
        if (rte->relkind == RELKIND_MATVIEW)
            continue;

        // Skip EXCLUDED pseudo-relation in INSERT ... ON CONFLICT
        if (parsetree->onConflict &&
            rt_index == parsetree->onConflict->exclRelIndex)
            continue;

        // Skip if table is not referenced in the query
        if (rt_index != parsetree->resultRelation &&
            !rangeTableEntry_used((Node *) parsetree, rt_index, 0))
            continue;

        // Skip new result relations introduced by ApplyRetrieveRule
        if (rt_index == parsetree->resultRelation &&
            rt_index != origResultRelation)
            continue;

        rel = table_open(rte->relid, NoLock);

        // Collect applicable RIR rules (SELECT rules only)
        rules = rel->rd_rules;
        if (rules != NULL)
        {
            locks = NIL;
            for (i = 0; i < rules->numLocks; i++)
            {
                rule = rules->rules[i];
                if (rule->event != CMD_SELECT)
                    continue;

                locks = lappend(locks, rule);
            }

            // Apply rules if found, with recursion detection
            if (locks != NIL)
            {
                if (list_member_oid(activeRIRs, RelationGetRelid(rel)))
                    ereport(ERROR, /* infinite recursion detected */);

                activeRIRs = lappend_oid(activeRIRs, RelationGetRelid(rel));

                foreach(l, locks)
                {
                    rule = lfirst(l);
                    parsetree = ApplyRetrieveRule(parsetree, rule, rt_index,
                                                 rel, activeRIRs);
                }

                activeRIRs = list_delete_last(activeRIRs);
            }
        }

        table_close(rel, NoLock);
    }

    // Recurse into CTE subqueries
    foreach(lc, parsetree->cteList)
    {
        CommonTableExpr *cte = (CommonTableExpr *) lfirst(lc);

        cte->ctequery = (Node *)
            fireRIRrules((Query *) cte->ctequery, activeRIRs);

        parsetree->hasRowSecurity |= ((Query *) cte->ctequery)->hasRowSecurity;
    }

    // Process sublinks if present
    if (parsetree->hasSubLinks)
    {
        fireRIRonSubLink_context context;

        context.activeRIRs = activeRIRs;
        context.hasRowSecurity = false;

        query_tree_walker(parsetree, fireRIRonSubLink, (void *) &context,
                         QTW_IGNORE_RC_SUBQUERIES);

        parsetree->hasRowSecurity |= context.hasRowSecurity;
    }

    // Apply row-level security policies
    rt_index = 0;
    foreach(lc, parsetree->rtable)
    {
        RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
        Relation rel;
        List *securityQuals;
        List *withCheckOptions;
        bool hasRowSecurity;
        bool hasSubLinks;

        ++rt_index;

        // Only normal relations can have RLS policies
        if (rte->rtekind != RTE_RELATION ||
            (rte->relkind != RELKIND_RELATION &&
             rte->relkind != RELKIND_PARTITIONED_TABLE))
            continue;

        rel = table_open(rte->relid, NoLock);

        // Get row security policies for this relation
        get_row_security_policies(parsetree, rte, rt_index,
                                 &securityQuals, &withCheckOptions,
                                 &hasRowSecurity, &hasSubLinks);

        if (securityQuals != NIL || withCheckOptions != NIL)
        {
            if (hasSubLinks)
            {
                // Handle sublinks in security policies with recursion detection
                if (list_member_oid(activeRIRs, RelationGetRelid(rel)))
                    ereport(ERROR, /* infinite recursion in policy */);

                activeRIRs = lappend_oid(activeRIRs, RelationGetRelid(rel));

                // Process sublinks in security quals
                // ... (acquire locks and fire RIR rules)

                activeRIRs = list_delete_last(activeRIRs);
            }

            // Add security quals to the RTE
            rte->securityQuals = list_concat(securityQuals, rte->securityQuals);
            parsetree->withCheckOptions = list_concat(withCheckOptions,
                                                     parsetree->withCheckOptions);
        }

        if (hasRowSecurity)
            parsetree->hasRowSecurity = true;
        if (hasSubLinks)
            parsetree->hasSubLinks = true;

        table_close(rel, NoLock);
    }

    return parsetree;
}
```