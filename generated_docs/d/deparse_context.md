# deparse_context

## Location
src/backend/utils/adt/ruleutils.c: 128 - 184

## Overview
deparse_context is a central data structure used in PostgreSQL's rule utility module to manage context information needed when deparsing (converting internal query structures back to SQL text).

## Definition

```c
typedef struct
{
	List	   *rtable;			/* List of RangeTblEntry nodes */
	List	   *rtable_names;	/* Parallel list of names for RTEs */
	List	   *rtable_columns; /* Parallel list of deparse_columns structs */
	List	   *subplans;		/* List of Plan trees for SubPlans */
	List	   *ctes;			/* List of CommonTableExpr nodes */
	AppendRelInfo **appendrels; /* Array of AppendRelInfo nodes, or NULL */
	/* Workspace for column alias assignment: */
	bool		unique_using;	/* Are we making USING names globally unique */
	List	   *using_names;	/* List of assigned names for USING columns */
	/* Remaining fields are used only when deparsing a Plan tree: */
	Plan	   *plan;			/* immediate parent of current expression */
	List	   *ancestors;		/* ancestors of plan */
	Plan	   *outer_plan;		/* outer subnode, or NULL if none */
	Plan	   *inner_plan;		/* inner subnode, or NULL if none */
	List	   *outer_tlist;	/* referent for OUTER_VAR Vars */
	List	   *inner_tlist;	/* referent for INNER_VAR Vars */
	List	   *index_tlist;	/* referent for INDEX_VAR Vars */
	/* Special namespace representing a function signature: */
	char	   *funcname;
	int			numargs;
	char	  **argnames;
} deparse_namespace;
```
## Detailed Description
The deparse_context structure serves as the primary coordination mechanism for PostgreSQL's SQL deparsing functionality in ruleutils.c. This structure maintains all the contextual information necessary to convert PostgreSQL's internal query tree representation back into readable SQL text format. It is used extensively throughout the rule system for generating views, rules, triggers, and other SQL constructs from their internal representations.

The structure manages output formatting through pretty-printing controls, maintains namespace information for proper variable resolution, and tracks the current parsing state across nested query levels. It supports both Query tree deparsing and PlannedStmt tree deparsing with different field usage patterns.

## Parameters / Member Variables
- : StringInfo buffer where the generated SQL text is accumulated
- : List of deparse_namespace nodes providing variable resolution context for nested query levels
- : Tuple descriptor for the view's output columns (used only at top level of view deparsing)
- : Current query level's SELECT target list for column reference resolution
- : Current query level's WINDOW clause specifications
- : Bit flags controlling various pretty-printing formatting options
- : Maximum line length for output formatting (-1 for unlimited)
- : Current indentation depth for pretty-printed output
- : Boolean flag indicating whether variable references should include table prefixes
- : Boolean flag indicating whether output column names matter for current context
- : Boolean flag indicating if currently deparsing a GROUP BY clause
- : Boolean flag indicating if deparsing a simple variable in ORDER BY context
- : Bitmapset for mapping child relation variables back to their parent relations in inheritance scenarios

## Dependencies
- Functions called/Symbols referenced:
  - AppendRelInfo (for inheritance relationship mapping)
  - StringInfo (for output buffer management)
  - List (for namespace and clause management)
  - TupleDesc (for result descriptor handling)
  - Bitmapset (for relation mapping)

- Called from (representative examples):
  - pg_get_triggerdef_worker
  - deparse_expression_pretty
  - get_rtable_name
  - make_ruledef
  - get_query_def
  - get_select_query_def
  - get_rule_expr
  - get_from_clause

## Notes and Other Information
- This structure is fundamental to PostgreSQL's ability to reconstruct SQL text from parsed query trees
- The context is typically initialized once and passed down through recursive deparsing calls
- Different fields are relevant depending on whether deparsing Query trees vs PlannedStmt trees
- The namespace management allows proper handling of nested subqueries and CTEs
- Pretty-printing capabilities enable generation of both compact and human-readable SQL output
- Used extensively in view definition generation, rule display, and query explanation features