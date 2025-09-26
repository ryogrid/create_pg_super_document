# deparse_context

## Location
[src/backend/utils/adt/ruleutils.c:128-184](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/ruleutils.c#L128-L184)

## Overview
deparse_context is a central data structure used in PostgreSQL's rule utility module to manage context information needed when deparsing (converting internal query structures back to SQL text).

## Definition

```c
typedef struct
{
	StringInfo	buf;			/* output buffer to append to */
	List	   *namespaces;		/* List of deparse_namespace nodes */
	TupleDesc	resultDesc;		/* if top level of a view, the view's tupdesc */
	List	   *targetList;		/* Current query level's SELECT targetlist */
	List	   *windowClause;	/* Current query level's WINDOW clause */
	int			prettyFlags;	/* enabling of pretty-print functions */
	int			wrapColumn;		/* max line length, or -1 for no limit */
	int			indentLevel;	/* current indent level for pretty-print */
	bool		varprefix;		/* true to print prefixes on Vars */
	bool		colNamesVisible;	/* do we care about output column names? */
	bool		inGroupBy;		/* deparsing GROUP BY clause? */
	bool		varInOrderBy;	/* deparsing simple Var in ORDER BY? */
	Bitmapset  *appendparents;	/* if not null, map child Vars of these relids
								 * back to the parent rel */
} deparse_context;
```
## Detailed Description
The deparse_context structure serves as the primary coordination mechanism for PostgreSQL's SQL deparsing functionality in ruleutils.c. This structure maintains all the contextual information necessary to convert PostgreSQL's internal query tree representation back into readable SQL text format. It is used extensively throughout the rule system for generating views, rules, triggers, and other SQL constructs from their internal representations.

The structure manages output formatting through pretty-printing controls, maintains namespace information for proper variable resolution, and tracks the current parsing state across nested query levels. It supports both Query tree deparsing and PlannedStmt tree deparsing with different field usage patterns.

## Parameters / Member Variables
- `buf`: StringInfo buffer where the generated SQL text is accumulated
- `namespaces`: List of deparse_namespace nodes providing variable resolution context for nested query levels
- `resultDesc`: Tuple descriptor for the view's output columns (used only at top level of view deparsing)
- `targetList`: Current query level's SELECT target list for column reference resolution
- `windowClause`: Current query level's WINDOW clause specifications
- `prettyFlags`: Bit flags controlling various pretty-printing formatting options
- `wrapColumn`: Maximum line length for output formatting (-1 for unlimited)
- `indentLevel`: Current indentation depth for pretty-printed output
- `varprefix`: Boolean flag indicating whether variable references should include table prefixes
- `colNamesVisible`: Boolean flag indicating whether output column names matter for current context
- `inGroupBy`: Boolean flag indicating if currently deparsing a GROUP BY clause
- `varInOrderBy`: Boolean flag indicating if deparsing a simple variable in ORDER BY context
- `appendparents`: Bitmapset for mapping child relation variables back to their parent relations in inheritance scenarios

## Dependencies
- Functions called/Symbols referenced:
  - [AppendRelInfo](../A/AppendRelInfo.md) (for inheritance relationship mapping)
  - StringInfo (for output buffer management)
  - [List](../L/List.md) (for namespace and clause management)
  - [TupleDesc](../T/TupleDesc.md) (for result descriptor handling)
  - [Bitmapset](../B/Bitmapset.md) (for relation mapping)

- Called from (representative examples):
  - [pg_get_triggerdef_worker](../p/pg_get_triggerdef_worker.md)
  - [deparse_expression_pretty](deparse_expression_pretty.md)
  - [get_rtable_name](../g/get_rtable_name.md)
  - [make_ruledef](../m/make_ruledef.md)
  - [get_query_def](../g/get_query_def.md)
  - [get_select_query_def](../g/get_select_query_def.md)
  - [get_rule_expr](../g/get_rule_expr.md)
  - [get_from_clause](../g/get_from_clause.md)

## Notes and Other Information
- This structure is fundamental to PostgreSQL's ability to reconstruct SQL text from parsed query trees
- The context is typically initialized once and passed down through recursive deparsing calls
- Different fields are relevant depending on whether deparsing Query trees vs PlannedStmt trees
- The namespace management allows proper handling of nested subqueries and CTEs
- Pretty-printing capabilities enable generation of both compact and human-readable SQL output
- Used extensively in view definition generation, rule display, and query explanation features