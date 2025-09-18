# fireRIRrules

## Location
src/backend/rewrite/rewriteHandler.c: 1982 - 2310

## Overview
Applies all RIR (Rules Instead Rewrite) rules on each range table entry in the given query, handling view expansion, rule recursion detection, and row-level security policies.

## Definition


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
  - rewriteSearchAndCycle
  - rt_fetch
  - rangeTableEntry_used
  - table_open
  - ApplyRetrieveRule
  - get_row_security_policies
  - acquireLocksOnSubLinks
  - fireRIRonSubLink
  - query_tree_walker
  - expression_tree_walker
  - list_member_oid
  - lappend_oid
  - list_delete_last
  - list_concat
- Called from (representative examples):
  - QueryRewrite
  - ApplyRetrieveRule
  - fireRIRonSubLink (for subqueries)

## Notes and Other Information
- Central function in PostgreSQL's rule rewriting system for view expansion
- Handles complex scenarios like materialized views, EXCLUDED pseudo-relations in UPSERT
- Implements sophisticated recursion detection to prevent infinite loops
- Processes row-level security policies as a final step after rule application
- Maintains hasRowSecurity and hasSubLinks flags throughout the query tree
- Uses special handling for range table entries that are not referenced in the query
- Skips materialized views to prevent inappropriate expansion during queries