# get_parse_rowmark

## Location
[src/backend/parser/parse_relation.c:3459-3482](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/parser/parse_relation.c#L3459-L3482)

## Overview
Searches a Query's rowMarks list to find and return the RowMarkClause for a specified range table entry.

## Definition

```c
RowMarkClause *
get_parse_rowmark(Query *qry, Index rtindex)
```
## Detailed Description
This function searches through a Query structure's rowMarks list to locate a RowMarkClause that corresponds to a specific range table index. Row marking is PostgreSQL's mechanism for implementing row-level locking semantics such as FOR UPDATE and FOR SHARE clauses in SQL queries.

The function performs a linear search through the rowMarks list, comparing each RowMarkClause's rti (range table index) field with the requested rtindex. This is used to determine what type of row-level locking should be applied to a particular relation in the query.

Row marks are essential for:
- Implementing SELECT FOR UPDATE/SHARE semantics
- Coordinating concurrent access to rows
- Ensuring proper locking behavior in complex queries
- Managing isolation levels and transaction semantics

## Parameters / Member Variables
- : Query structure containing the rowMarks list to search
- : Range table index of the relation to find row marking information for

## Dependencies
- Functions called/Symbols referenced:
  - [RowMarkClause](../R/RowMarkClause.md) (structure type used in the search)
  - Standard PostgreSQL list iteration macros (foreach, lfirst)
- Called from (representative examples):
  - [applyLockingClause](../a/applyLockingClause.md) (query analysis for locking clauses)
  - [AcquireRewriteLocks](../A/AcquireRewriteLocks.md) (rewrite rule processing)
  - [ApplyRetrieveRule](../A/ApplyRetrieveRule.md) (rule application)
  - rt_fetch (via macro expansion)

## Notes and Other Information
- Returns NULL if no RowMarkClause is found for the specified range table index, indicating the relation is not selected FOR UPDATE/SHARE
- This function is crucial for proper implementation of SQL row-level locking semantics
- The linear search approach is appropriate given that rowMarks lists are typically small
- Used extensively in query rewriting and analysis phases to determine locking requirements for relations