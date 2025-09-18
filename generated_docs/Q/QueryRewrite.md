# QueryRewrite

## Location
src/backend/rewrite/rewriteHandler.c: 4411 - 4500

## Overview
Primary entry point to PostgreSQL's query rewriter that transforms a single query into zero or more rewritten queries by applying rules and handling view updates.

## Definition
```c
List *QueryRewrite(Query *parsetree)
```

## Detailed Description
QueryRewrite serves as the main interface to PostgreSQL's query rewrite system, orchestrating the complete rewriting process for top-level queries. It operates in three distinct phases to ensure proper rule application and query processing.

**Phase 1**: Applies all non-SELECT rules by calling RewriteQuery, which handles DML rule processing, view updatability checks, and recursive rule application. This may produce zero, one, or multiple result queries.

**Phase 2**: Applies all RIR (Range table Introspection and Rewrite) rules to each resulting query using fireRIRrules. RIR rules handle view expansion for SELECT operations and other range table transformations. Each query is also tagged with the original query ID for tracking purposes.

**Phase 3**: Determines command tag ownership among the resulting queries. The original query sets the command tag if it remains in the result set. Otherwise, the last INSTEAD rule query of the same command type as the original sets the tag. This ensures proper command completion reporting to the client.

The function requires that input queries come directly from the parser or have been processed by AcquireRewriteLocks to ensure proper locking is in place.

## Parameters / Member Variables
- `parsetree`: The Query node representing the top-level original query to be rewritten

## Dependencies
- Functions called/Symbols referenced:
  - [RewriteQuery](../R/RewriteQuery.md)
  - [fireRIRrules](../f/fireRIRrules.md)
  - lappend
- Called from (representative examples):
  - [ExecCreateTableAs](../E/ExecCreateTableAs.md) (src/backend/commands/createas.c:291)
  - [ExplainQuery](../E/ExplainQuery.md) (src/backend/commands/explain.c:321)
  - [PerformCursorOpen](../P/PerformCursorOpen.md) (src/backend/commands/portalcmds.c:80)
  - [pg_rewrite_query](../p/pg_rewrite_query.md) (src/backend/tcop/postgres.c:827)
  - [refresh_matview_datafill](../r/refresh_matview_datafill.md) (src/backend/commands/matview.c:401)

## Notes and Other Information
- Only accepts top-level original queries (must have querySource == QSRC_ORIGINAL and canSetTag == true)
- Preserves the original query ID across all generated queries for tracking and logging purposes
- Implements the canSetTag protocol to ensure exactly one query (or none) can set the command result tag
- The three-phase approach ensures rules are applied in the correct order: DML rules first, then RIR rules for view expansion
- May return an empty list if DO INSTEAD NOTHING rules eliminate all queries
- [Command](../C/Command.md) tag determination follows specific precedence: original query first, then last qualifying INSTEAD rule query
- Input queries must have appropriate locks acquired via AcquireRewriteLocks before calling this function
- The function is assertion-protected to ensure only one query can set the command tag in the result list