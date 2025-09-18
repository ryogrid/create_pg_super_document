# AppendRelInfo

## Location
src/include/nodes/pathnodes.h: 2959 - 3016

## Overview
AppendRelInfo provides the mapping information needed to translate between parent and child relations in inheritance hierarchies and UNION-ALL subqueries, enabling PostgreSQL to expand inheritable tables into lists of child tables.

## Definition


## Detailed Description
AppendRelInfo structures are created when PostgreSQL expands an inheritable table or UNION-ALL subselect into an "append relation" - essentially a list of child relations that must be processed together. Each AppendRelInfo maps one child relation to its parent, providing all the translation information needed to convert references to parent columns into appropriate references to child columns.

The structure handles two main scenarios: table inheritance (where parent and child are regular relations with potentially different column layouts) and UNION ALL queries (where parent and child are subqueries that need expression translation). The data structure assumes append-relation members are single base relations, which works for inheritance but limits UNION ALL optimization when member subqueries contain joins.

These structures are stored in the PlannerInfo's append_rel_list and indexed by append_rel_array for efficient lookup during planning.

## Parameters / Member Variables
- : Node tag for identification
- : Range table index of the append parent relation
- : Range table index of the append child relation
- : OID of parent's composite row type (InvalidOid for UNION ALL)
- : OID of child's composite row type (InvalidOid for UNION ALL)
- : List of expressions/Vars mapping parent columns to child columns
- : Length of the parent_colnos array
- : Array mapping child column numbers back to parent column numbers
- : OID of parent table (InvalidOid for UNION ALL, used for error messages)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (node identification)
  - Index (relation identifiers)
  - Oid (object identifiers)
  - [List](../L/List.md) (generic list structure)
  - AttrNumber (attribute number type)

- Called from (representative examples):
  - [make_append_rel_info](../m/make_append_rel_info.md)
  - [adjust_appendrel_attrs](../a/adjust_appendrel_attrs.md)
  - [set_append_rel_size](../s/set_append_rel_size.md)
  - [expand_partitioned_rtentry](../e/expand_partitioned_rtentry.md)
  - [pull_up_union_leaf_queries](../p/pull_up_union_leaf_queries.md)

## Notes and Other Information
- Critical for table inheritance and UNION ALL query processing
- Enables column-level translation between parent and child relations
- Supports both forward translation (parent to child) and reverse translation (child to parent)
- Handles dropped columns in inheritance scenarios by using NULL entries
- Used extensively in partitioned table planning and query rewriting
- The "inh" flag in RTEs indicates presence of append relationships
- Limitation: assumes append-relation members are single baserels, preventing some UNION ALL optimizations
- Essential for partitionwise join planning and constraint exclusion