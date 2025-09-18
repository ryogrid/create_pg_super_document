# ParseState

## Location
src/include/parser/parse_node.h: 190 - 283

## Overview
ParseState is the central state structure used during SQL parsing that maintains context information about the current parsing operation, including namespace resolution, range tables, and various parsing flags.

## Definition


## Detailed Description
ParseState serves as the central context structure during SQL query parsing in PostgreSQL. It maintains all the necessary state information to resolve names, track relations, and provide context for expression parsing. The structure forms a stack through the parentParseState link, allowing nested query contexts (such as subqueries) to access outer scope information while maintaining their own local state.

The parser uses this structure to track range table entries (RTEs), namespace visibility rules, join information, and various parsing flags that affect how expressions and names are interpreted. It also provides hooks for extensibility, allowing custom parsing behavior to be injected at key points.

## Parameters / Member Variables
- : Link to parent parsing context for nested queries
- : Original SQL text being parsed (may be NULL)
- : List of RangeTblEntry structures representing FROM clause items
- : Permission information for each relation in the range table
- : Join expressions for RTE_JOIN entries
- : Bitmapsets indicating which outer joins cause nulling
- : List of join items that will become the FromExpr's fromlist
- : Currently visible namespace items (List of ParseNamespaceItem)
- : Whether LATERAL-only items are currently visible
- : Namespace for common table expressions (CTEs)
- : CTEs not yet visible in current scope
- : The CTE containing this query, if any
- : Target relation for INSERT/UPDATE/DELETE/MERGE
- : Namespace item for target relation
- : Whether to process assignments like INSERT vs UPDATE
- : Raw window clause definitions
- : Type of expression currently being parsed
- : Next result number to assign in target list
- : Target list entries for multi-assignment
- : FOR UPDATE/FOR SHARE information
- : Whether parent query locked this subquery
- : Whether to resolve unknown types as text
- : Query environment including references to enclosing environments
- : Flag indicating presence of aggregate functions
- : Flag indicating presence of window functions
- : Flag indicating set-returning functions in target list
- : Flag indicating presence of subqueries
- : Flag indicating presence of data-modifying CTEs
- : Most recent set-returning function/operator found
- : Optional hook for preprocessing column references
- : Optional hook for postprocessing column references
- : Optional hook for parameter references
- : Optional hook for parameter coercion
- : Common state passed to hook functions

## Dependencies
- Functions called/Symbols referenced:
  - ParseNamespaceItem (used in p_namespace list)
  - ParseExprKind (for p_expr_kind field)
  - QueryEnvironment (for p_queryEnv field)
  - CommonTableExpr (for p_parent_cte field)
  - RangeTblEntry (for p_rtable list)
  - RTEPermissionInfo (for p_rteperminfos list)
  - Various hook function types

- Called from (representative examples):
  - transformDeleteStmt
  - transformInsertStmt
  - transformUpdateStmt
  - transformSelectStmt
  - transformFromClause
  - Many parser functions in parse_*.c files

## Notes and Other Information
ParseState is fundamental to PostgreSQL's parsing architecture and is used throughout the parser subsystem. The structure supports nested contexts through the parentParseState link, enabling proper scoping for subqueries and CTEs. The various flags (p_hasAggs, p_hasWindowFuncs, etc.) are used later during planning to determine what optimizations and execution strategies are appropriate. The hook mechanism provides extensibility for custom parsing behavior, which is used by PL/pgSQL and other extensions.