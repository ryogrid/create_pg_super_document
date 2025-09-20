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
  - table_open
  - [ApplyRetrieveRule](../A/ApplyRetrieveRule.md)
  - [get_row_security_policies](../g/get_row_security_policies.md)
  - [acquireLocksOnSubLinks](../a/acquireLocksOnSubLinks.md)
  - [fireRIRonSubLink](fireRIRonSubLink.md)
  - query_tree_walker
  - expression_tree_walker
  - [list_member_oid](../l/list_member_oid.md)
  - lappend_oid
  - list_delete_last
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