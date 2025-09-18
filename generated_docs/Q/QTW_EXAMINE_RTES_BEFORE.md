# QTW_EXAMINE_RTES_BEFORE

## Location
src/include/nodes/nodeFuncs.h: 27 - 28

## Overview
A flag bit constant used to control the behavior of query_tree_walker and query_tree_mutator functions, specifically instructing them to examine Range Table Entry (RTE) nodes before processing their contents.

## Definition


## Detailed Description
QTW_EXAMINE_RTES_BEFORE is a bit flag with value 0x10 (16 in decimal) that controls the traversal order when walking through PostgreSQL's query tree structures. When this flag is set, the walker function will examine Range Table Entry nodes before descending into their contents. This is particularly useful for operations that need to process the RTE metadata or structure before analyzing the expressions and subqueries contained within the RTE.

Range Table Entries (RTEs) represent table references in a query's FROM clause, including base tables, subqueries, functions, and other relation types. The timing of when these nodes are examined can be critical for certain optimization and rewriting operations.

## Parameters / Member Variables
- Value:  (hexadecimal) - The bit flag value used in bitwise operations with other QTW flags

## Dependencies
- Functions called/Symbols referenced:
  - (This is a constant definition - no function calls)
- Called from (representative examples):
  - range_table_entry_walker_impl (src/backend/nodes/nodeFuncs.c:2820)
  - flatten_unplanned_rtes (src/backend/optimizer/plan/setrefs.c:488)
  - flatten_rtes_walker (src/backend/optimizer/plan/setrefs.c:519)
  - contain_outer_selfref_walker (src/backend/optimizer/plan/subselect.c:1124)
  - IncrementVarSublevelsUp_walker (src/backend/rewrite/rewriteManip.c:841)
  - IncrementVarSublevelsUp (src/backend/rewrite/rewriteManip.c:865)
  - IncrementVarSublevelsUp_rtable (src/backend/rewrite/rewriteManip.c:884)

## Notes and Other Information
- This flag is part of a family of QTW (Query Tree Walker) flags defined in src/include/nodes/nodeFuncs.h
- Can be combined with other QTW flags using bitwise OR operations
- Commonly used in query rewriting and optimization phases
- The counterpart flag QTW_EXAMINE_RTES_AFTER (0x20) provides the opposite behavior - examining RTEs after their contents
- Essential for maintaining correct semantics when transforming query trees that involve complex nested structures