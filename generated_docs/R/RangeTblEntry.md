# RangeTblEntry

## Location
src/include/nodes/parsenodes.h: 1038 - 1251

## Overview
RangeTblEntry is a fundamental data structure in PostgreSQL that represents an entry in a range table, which contains information about each table, subquery, function, or other data source referenced in a SQL query.

## Definition


## Detailed Description
RangeTblEntry is a polymorphic structure that represents different kinds of range table entries in PostgreSQL's query processing system. The rtekind field determines which subset of fields are valid for a particular instance. This structure is central to PostgreSQL's query representation and is used throughout the parser, planner, and executor phases.

The structure supports various types of data sources including regular relations (tables/views), subqueries, joins, functions, VALUES clauses, Common Table Expressions (CTEs), Ephemeral Named Relations (ENRs), and table functions. Each type uses different subsets of the available fields.

## Parameters / Member Variables
- : NodeTag identifying this as a RangeTblEntry node
- : User-written alias clause for the range table entry
- : Expanded reference names (effective column names after alias resolution)
- : Specifies the kind of RTE (relation, subquery, join, function, etc.)
- : OID of the relation (for relation RTEs)
- : Whether inheritance should be considered for relation references
- : Kind of relation from pg_class.relkind
- : Lock level required on the relation (LOCKMODE stored as int)
- : 1-based index into RTEPermissionInfo list, 0 if no permission check needed
- : Sampling information for table sampling clauses
- : The sub-query for subquery RTEs
- : Whether this is from a security_barrier view
- : Type of join for join RTEs
- : Number of merged columns in JOIN USING
- : List of alias variable expansions for join columns
- : Physical column numbers from left join input
- : Physical column numbers from right join input
- : Alias clause attached directly to JOIN/USING
- : List of RangeTblFunction nodes for function RTEs
- : Whether function is called WITH ORDINALITY
- : TableFunc node for table function RTEs
- : List of expression lists for VALUES RTEs
- : Name of the WITH list item for CTE RTEs
- : Number of query levels up for CTE references
- : Whether this is a recursive self-reference in CTE
- : OID list of column type OIDs
- : List of column type modifiers
- : OID list of column collation OIDs
- : Name of ephemeral named relation
- : Estimated or actual tuple count for ENRs
- : Whether LATERAL was specified
- : Whether this RTE appears in the FROM clause
- : Security barrier qualifiers to apply

## Dependencies
- Functions called/Symbols referenced:
  - [Alias](../A/Alias.md)
  - [RTEKind](RTEKind.md)
  - [TableSampleClause](../T/TableSampleClause.md)
  - JoinType
  - [TableFunc](../T/TableFunc.md)
  - Cardinality
- Called from (representative examples):
  - [Query](../Q/Query.md) parser functions
  - Planner routines
  - Executor initialization

## Notes and Other Information
- The structure uses conditional compilation attributes (pg_node_attr) to control serialization and query fingerprinting behavior
- Different RTEKind values activate different subsets of fields, making this a polymorphic structure
- Lock modes are stored as integers to avoid including lock-related headers
- The structure handles complex scenarios like view expansion, join alias resolution, and CTE recursion
- Special handling exists for dropped columns in ENRs (represented as zero values)
- [Query](../Q/Query.md) jumble attributes are used to exclude certain fields from query fingerprinting for performance optimization