# PrepareQuery

## Location
src/backend/commands/prepare.c: 56 - 146

## Overview
Implements the 'PREPARE' utility statement, creating a named prepared statement by parsing, analyzing, and storing a SQL statement with optional parameter types for later execution.

## Definition


## Detailed Description
PrepareQuery processes a PREPARE SQL statement by creating a cached plan source that can be executed multiple times with different parameter values. The function validates the statement name, wraps the query in a RawStmt node, performs parse analysis and query rewriting, and stores the resulting prepared statement for future use. It handles parameter type resolution either from explicitly specified types or by inferring them from the query context.

## Parameters / Member Variables
- : Parse state containing parsing context and source text information
- : PrepareStmt node containing the statement name, query, and optional parameter types
- : Starting location of the statement in the source text
- : Length of the statement in the source text

## Dependencies
- Functions called/Symbols referenced:
  - makeNode (creates RawStmt)
  - CreateCachedPlan (creates cached plan source)
  - CreateCommandTag (generates command tag)
  - typenameTypeId (resolves type names to OIDs)
  - pg_analyze_and_rewrite_varparams (performs analysis and rewriting)
  - CompleteCachedPlan (finalizes cached plan)
  - StorePreparedStatement (stores the prepared statement)
- Called from (representative examples):
  - standard_ProcessUtility (utility command processing)

## Notes and Other Information
- Disallows empty statement names to avoid conflicts with protocol-level unnamed statements
- Supports parameter type inference when explicit types are not provided
- Creates reusable cached plans that can improve performance for repeated executions
- Allows parallel execution mode for compatible queries
- Part of PostgreSQL's prepared statement infrastructure for optimizing repeated query execution