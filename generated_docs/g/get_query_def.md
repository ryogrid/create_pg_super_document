# get_query_def

## Location
src/backend/utils/adt/ruleutils.c: 5437 - 5519

## Overview
Converts a Query parse tree back into readable SQL text by dispatching to appropriate command-specific formatting functions based on the query's command type.

## Definition


## Detailed Description
The  function serves as the central dispatcher for converting PostgreSQL's internal Query parse trees back into human-readable SQL text. It sets up the deparse context with formatting parameters and namespace information, then routes the query to the appropriate specialized function based on its command type (SELECT, INSERT, UPDATE, DELETE, MERGE, UTILITY, or NOTHING).

Before deparsing begins, the function performs important setup tasks:
- Guards against stack overflow and interrupts for deeply nested or long-running operations
- Acquires necessary locks on referenced relations using AccessShareLock (read-only)
- Initializes a deparse_context structure with all formatting parameters
- Sets up namespace resolution for handling table and column references

The function uses a switch statement to dispatch to command-specific handlers like , , etc., ensuring that each SQL command type is formatted according to its specific syntax requirements.

## Parameters / Member Variables
- : Query parse tree to be converted back to SQL text
- : StringInfo buffer where the generated SQL text will be appended
- : List of outer-level deparse_namespace structures for nested query context
- : Optional tuple descriptor for SELECT queries, used to provide preferred column names for output
- : Boolean indicating whether column names should be visible in the current context
- : Bitmask of PRETTYFLAG_XXX options controlling formatting style
- : Maximum line length for wrapping, or -1 to disable line wrapping
- : Initial indentation level for the generated SQL

## Dependencies
- Functions called/Symbols referenced:
  - CHECK_FOR_INTERRUPTS
  - check_stack_depth
  - AcquireRewriteLocks
  - lcons
  - list_copy
  - set_deparse_for_query
  - get_select_query_def
  - get_update_query_def
  - get_insert_query_def
  - get_delete_query_def
  - get_merge_query_def
  - get_utility_query_def
  - CMD_SELECT, CMD_UPDATE, CMD_INSERT, CMD_DELETE, CMD_MERGE, CMD_NOTHING, CMD_UTILITY
- Called from (representative examples):
  - pg_get_querydef
  - make_ruledef
  - make_viewdef
  - get_with_clause
  - get_setop_query
  - get_insert_query_def
  - get_sublink_expr
  - get_from_clause_item

## Notes and Other Information
This function is a core component of PostgreSQL's rule system and query deparsing infrastructure. It's used extensively throughout the system for generating readable SQL from internal parse trees, particularly in view definitions, rule definitions, and query introspection. The function is designed to handle nested queries and maintains proper namespace resolution through the parentnamespace parameter. The locking mechanism ensures consistency when deparsing queries that reference database objects that might be modified concurrently.