# SortByDir

## Location
[src/include/nodes/parsenodes.h:50-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L50-L51)

## Overview
SortByDir is an enumeration type that defines sort ordering options used in ORDER BY clauses and CREATE INDEX statements to specify the direction of sorting.

## Definition

```c
typedef enum SortByNulls
{
	SORTBY_NULLS_DEFAULT,
	SORTBY_NULLS_FIRST,
	SORTBY_NULLS_LAST,
} SortByNulls;
```
## Detailed Description
SortByDir provides a standardized way to represent sort direction options in PostgreSQL's parser nodes. It encompasses the standard ascending and descending sort orders, as well as a default option and a special USING clause option for custom sorting operators. The enum is primarily used in query parsing and execution planning to maintain sort order information throughout the query processing pipeline.

## Parameters / Member Variables
- : Default sorting behavior (typically ascending)
- : Explicit ascending sort order
- : Explicit descending sort order  
- : Custom sorting using a specified operator (not permitted in CREATE INDEX statements)

## Dependencies
- Functions called/Symbols referenced:
  - None (enum definition)
- Called from (representative examples):
  - [SortBy](SortBy.md) (in sortby_dir field)
  - [IndexElem](../I/IndexElem.md) (in ordering field)

## Notes and Other Information
- Located in src/include/nodes/parsenodes.h:44-50
- The SORTBY_USING option allows for custom sort operators but is restricted from use in CREATE INDEX statements
- Commonly paired with SortByNulls enum to handle NULL value ordering
- Used throughout the query parser and planner to maintain sort order semantics