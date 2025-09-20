# SetOperation

## Location
[src/include/nodes/parsenodes.h:2114-2115](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2114-L2115)

## Overview
SetOperation is an enumeration that defines the different types of set operations (UNION, INTERSECT, EXCEPT) that can be used to combine results from multiple SELECT statements in PostgreSQL.

## Definition

```c
typedef enum SetOperation
{
	SETOP_NONE = 0,
	SETOP_UNION,
	SETOP_INTERSECT,
	SETOP_EXCEPT,
} SetOperation;
```
## Detailed Description
SetOperation specifies the type of set operation used to combine multiple SELECT statements in compound queries. These operations follow SQL standard set theory semantics, allowing queries to combine, intersect, or subtract result sets from multiple component queries. The enumeration is used in PostgreSQL's parse tree to represent the structure of compound SELECT statements, where internal nodes represent set operations and leaf nodes represent individual SELECT statements.

## Parameters / Member Variables
- : Indicates no set operation (used for simple SELECT statements)
- : Represents UNION operation, combining all rows from both queries (with or without duplicates)
- : Represents INTERSECT operation, returning only rows that appear in both queries
- : Represents EXCEPT operation, returning rows from the first query that don't appear in the second

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is an enumeration type)
- Called from (representative examples):
  - SelectStmt (src/include/nodes/parsenodes.h:2158)
  - SetOperationStmt (src/include/nodes/parsenodes.h:2188)

## Notes and Other Information
- [SetOperation](SetOperation.md) is fundamental to PostgreSQL's compound query processing
- SETOP_NONE is used for leaf nodes in the SelectStmt tree structure
- Set operations can be combined with ALL modifier to control duplicate handling
- The enumeration is used in both SelectStmt (for parsing) and SetOperationStmt (for execution planning)
- PostgreSQL represents compound queries as trees where internal nodes are set operations
- Each set operation type has distinct semantics following SQL standard specifications
- UNION is the most commonly used set operation, combining results from multiple sources
- All set operations require compatible column types and counts between operand queries