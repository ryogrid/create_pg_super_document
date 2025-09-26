# StatsElem

## Location
[src/include/nodes/parsenodes.h:3403-3408](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L3403-L3408)

## Overview
StatsElem represents a single column or expression element within a CREATE STATISTICS statement, specifying what data should be analyzed for extended statistics collection.

## Definition

```c
typedef struct StatsElem
{
	NodeTag		type;
	char	   *name;			/* name of attribute to index, or NULL */
	Node	   *expr;			/* expression to index, or NULL */
} StatsElem;
```
## Detailed Description
StatsElem is a parse tree node that represents individual elements (columns or expressions) in a CREATE STATISTICS statement's target list. It can represent either a simple column reference by name or a complex expression. The structure follows a mutually exclusive pattern where exactly one of 'name' or 'expr' is non-NULL, determining whether the element refers to a table column or a computed expression.

This flexibility allows PostgreSQL's extended statistics to work not just on simple columns but also on computed values, enabling more sophisticated query optimization through better cardinality estimates on complex predicates.

## Parameters / Member Variables
- : Standard NodeTag for PostgreSQL parse tree nodes
- : Name of the table column when referencing a simple attribute (NULL for expressions)
- : Parse tree node representing the expression to analyze (NULL for simple columns)

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (parse tree infrastructure)
  - Node (base parse tree node type)

- Called from (representative examples):
  - CreateStatistics (statistics creation processing)
  - transformStatsStmt (parse transformation and validation)
  - generateClonedExtStatsStmt (table cloning operations)
  - ChooseExtendedStatisticNameAddition (automatic naming)

## Notes and Other Information
- Follows mutually exclusive design: exactly one of 'name' or 'expr' is non-NULL
- Simple column case: name contains column name, expr is NULL
- Expression case: name is NULL, expr contains the expression parse tree
- Used within the exprs list of CreateStatsStmt structures
- Enables extended statistics on both simple columns and complex expressions
- Part of PostgreSQL's multi-column statistics infrastructure for better query optimization