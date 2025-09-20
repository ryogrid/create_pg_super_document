# IndexElem

## Location
[src/include/nodes/parsenodes.h:780-791](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L780-L791)

## Overview
IndexElem represents an index parameter used in CREATE INDEX statements and ON CONFLICT clauses, defining either a table column or expression to be indexed along with its indexing options.

## Definition

```c
typedef struct IndexElem
{
	NodeTag		type;
	char	   *name;			/* name of attribute to index, or NULL */
	Node	   *expr;			/* expression to index, or NULL */
	char	   *indexcolname;	/* name for index column; NULL = default */
	List	   *collation;		/* name of collation; NIL = default */
	List	   *opclass;		/* name of desired opclass; NIL = default */
	List	   *opclassopts;	/* opclass-specific options, or NIL */
	SortByDir	ordering;		/* ASC/DESC/default */
	SortByNulls nulls_ordering; /* FIRST/LAST/default */
} IndexElem;
```
## Detailed Description
IndexElem is a fundamental structure in PostgreSQL's parser nodes that represents individual elements (columns or expressions) to be indexed. It can represent either a simple column index (where name is specified and expr is NULL) or an expression index (where expr is specified and name is NULL). The structure encapsulates all the necessary information for creating an index on a particular element, including collation rules, operator classes, sorting preferences, and null handling behavior.

## Parameters / Member Variables
- : NodeTag identifier for this node type
- : Name of the table column to index (NULL for expression indexes)
- : Expression tree to index (NULL for simple column indexes)
- : Custom name for the index column (NULL uses default naming)
- : List specifying the collation to use (NIL for default)
- : List specifying the desired operator class (NIL for default)
- : Operator class-specific options (NIL if none)
- : Sort direction specification (ASC/DESC/default)
- : NULL value ordering specification (FIRST/LAST/default)

## Dependencies
- Functions called/Symbols referenced:
  - [SortByDir](../S/SortByDir.md) (enum for ordering direction)
  - SortByNulls (enum for null ordering)
- Called from (representative examples):
  - [ComputeIndexAttrs](../C/ComputeIndexAttrs.md) (builds index attribute information)
  - [ChooseIndexColumnNames](../C/ChooseIndexColumnNames.md) (determines column names for indexes)
  - [transformIndexConstraint](../t/transformIndexConstraint.md) (processes index constraints)
  - [transformIndexStmt](../t/transformIndexStmt.md) (transforms CREATE INDEX statements)

## Notes and Other Information
IndexElem is crucial for PostgreSQL's index creation process, serving as the intermediate representation between parsed SQL and the internal index structures. The structure supports both traditional column-based indexes and more complex expression-based indexes. When processing CREATE INDEX statements, the parser creates IndexElem nodes for each indexed element, which are later processed by the index creation subsystem to build the actual index structures.