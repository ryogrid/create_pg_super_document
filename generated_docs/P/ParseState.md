# ParseState

## Location
[src/include/parser/parse_node.h:190-283](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/parser/parse_node.h#L190-L283)

## Overview
ParseState is the central state structure used during SQL parsing that maintains context information about the current parsing operation, including namespace resolution, range tables, and various parsing flags.

## Definition

```c
struct ParseState
{
	ParseState *parentParseState;	/* stack link */
	const char *p_sourcetext;	/* source text, or NULL if not available */
	List	   *p_rtable;		/* range table so far */
	List	   *p_rteperminfos; /* list of RTEPermissionInfo nodes for each
								 * RTE_RELATION entry in rtable */
	List	   *p_joinexprs;	/* JoinExprs for RTE_JOIN p_rtable entries */
	List	   *p_nullingrels;	/* Bitmapsets showing nulling outer joins */
	List	   *p_joinlist;		/* join items so far (will become FromExpr
								 * node's fromlist) */
	List	   *p_namespace;	/* currently-referenceable RTEs (List of
								 * ParseNamespaceItem) */
	bool		p_lateral_active;	/* p_lateral_only items visible? */
	List	   *p_ctenamespace; /* current namespace for common table exprs */
	List	   *p_future_ctes;	/* common table exprs not yet in namespace */
	CommonTableExpr *p_parent_cte;	/* this query's containing CTE */
	Relation	p_target_relation;	/* INSERT/UPDATE/DELETE/MERGE target rel */
	ParseNamespaceItem *p_target_nsitem;	/* target rel's NSItem, or NULL */
	bool		p_is_insert;	/* process assignment like INSERT not UPDATE */
	List	   *p_windowdefs;	/* raw representations of window clauses */
	ParseExprKind p_expr_kind;	/* what kind of expression we're parsing */
	int			p_next_resno;	/* next targetlist resno to assign */
	List	   *p_multiassign_exprs;	/* junk tlist entries for multiassign */
	List	   *p_locking_clause;	/* raw FOR UPDATE/FOR SHARE info */
	bool		p_locked_from_parent;	/* parent has marked this subquery
										 * with FOR UPDATE/FOR SHARE */
	bool		p_resolve_unknowns; /* resolve unknown-type SELECT outputs as
									 * type text */

	QueryEnvironment *p_queryEnv;	/* curr env, incl refs to enclosing env */

	/* Flags telling about things found in the query: */
	bool		p_hasAggs;
	bool		p_hasWindowFuncs;
	bool		p_hasTargetSRFs;
	bool		p_hasSubLinks;
	bool		p_hasModifyingCTE;

	Node	   *p_last_srf;		/* most recent set-returning func/op found */

	/*
	 * Optional hook functions for parser callbacks.  These are null unless
	 * set up by the caller of make_parsestate.
	 */
	PreParseColumnRefHook p_pre_columnref_hook;
	PostParseColumnRefHook p_post_columnref_hook;
	ParseParamRefHook p_paramref_hook;
	CoerceParamHook p_coerce_param_hook;
	void	   *p_ref_hook_state;	/* common passthrough link for above */
};
```
## Detailed Description
ParseState serves as the central context structure during SQL query parsing in PostgreSQL. It maintains all the necessary state information to resolve names, track relations, and provide context for expression parsing. The structure forms a stack through the parentParseState link, allowing nested query contexts (such as subqueries) to access outer scope information while maintaining their own local state.

The parser uses this structure to track range table entries (RTEs), namespace visibility rules, join information, and various parsing flags that affect how expressions and names are interpreted. It also provides hooks for extensibility, allowing custom parsing behavior to be injected at key points.

## Parameters / Member Variables
- `*parentParseState`: Link to parent parsing context for nested queries
- `*p_sourcetext`: Original SQL text being parsed (may be NULL)
- `*p_rtable`: List of RangeTblEntry structures representing FROM clause items
- `*p_rteperminfos`: Permission information for each relation in the range table
- `*p_joinexprs`: Join expressions for RTE_JOIN entries
- `*p_nullingrels`: Bitmapsets indicating which outer joins cause nulling
- `*p_joinlist`: List of join items that will become the FromExpr's fromlist
- `*p_namespace`: Currently visible namespace items (List of ParseNamespaceItem)
- `p_lateral_active`: Whether LATERAL-only items are currently visible
- `*p_ctenamespace`: Namespace for common table expressions (CTEs)
- `*p_future_ctes`: CTEs not yet visible in current scope
- `*p_parent_cte`: The CTE containing this query, if any
- `p_target_relation`: Target relation for INSERT/UPDATE/DELETE/MERGE
- `*p_target_nsitem`: Namespace item for target relation
- `p_is_insert`: Whether to process assignments like INSERT vs UPDATE
- `*p_windowdefs`: Raw window clause definitions
- `p_expr_kind`: Type of expression currently being parsed
- `p_next_resno`: Next result number to assign in target list
- `*p_multiassign_exprs`: Target list entries for multi-assignment
- `*p_locking_clause`: FOR UPDATE/FOR SHARE information
- `p_locked_from_parent`: Whether parent query locked this subquery
- `p_resolve_unknowns`: Whether to resolve unknown types as text
- `*p_queryEnv`: Query environment including references to enclosing environments
- `p_hasAggs`: Flag indicating presence of aggregate functions
- `p_hasWindowFuncs`: Flag indicating presence of window functions
- `p_hasTargetSRFs`: Flag indicating set-returning functions in target list
- `p_hasSubLinks`: Flag indicating presence of subqueries
- `p_hasModifyingCTE`: Flag indicating presence of data-modifying CTEs
- `*p_last_srf`: Most recent set-returning function/operator found
- `p_pre_columnref_hook`: Optional hook for preprocessing column references
- `p_post_columnref_hook`: Optional hook for postprocessing column references
- `p_paramref_hook`: Optional hook for parameter references
- `p_coerce_param_hook`: Optional hook for parameter coercion
- `*p_ref_hook_state`: Common state passed to hook functions

## Dependencies
- Functions called/Symbols referenced:
  - [ParseNamespaceItem](ParseNamespaceItem.md) (used in p_namespace list)
  - [ParseExprKind](ParseExprKind.md) (for p_expr_kind field)
  - QueryEnvironment (for p_queryEnv field)
  - CommonTableExpr (for p_parent_cte field)
  - [RangeTblEntry](../R/RangeTblEntry.md) (for p_rtable list)
  - [RTEPermissionInfo](../R/RTEPermissionInfo.md) (for p_rteperminfos list)
  - Various hook function types

- Called from (representative examples):
  - [transformDeleteStmt](../t/transformDeleteStmt.md)
  - [transformInsertStmt](../t/transformInsertStmt.md)
  - [transformUpdateStmt](../t/transformUpdateStmt.md)
  - [transformSelectStmt](../t/transformSelectStmt.md)
  - [transformFromClause](../t/transformFromClause.md)
  - Many parser functions in parse_*.c files

## Notes and Other Information
ParseState is fundamental to PostgreSQL's parsing architecture and is used throughout the parser subsystem. The structure supports nested contexts through the parentParseState link, enabling proper scoping for subqueries and CTEs. The various flags (p_hasAggs, p_hasWindowFuncs, etc.) are used later during planning to determine what optimizations and execution strategies are appropriate. The hook mechanism provides extensibility for custom parsing behavior, which is used by PL/pgSQL and other extensions.