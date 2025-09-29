# sql_fn_parser_setup

## Location
[src/backend/executor/functions.c:265-277](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/functions.c#L265-L277)

## Overview
Sets up parser hooks for parsing a SQL function body by configuring column reference and parameter reference handlers.

## Definition

```c
void
sql_fn_parser_setup(struct ParseState *pstate, SQLFunctionParseInfoPtr pinfo)
```
## Detailed Description
This function configures a ParseState structure with the necessary hooks for parsing SQL function bodies. It sets up specialized handlers for column references and parameter references that are specific to SQL function parsing context. The function disables the pre-column reference hook, assigns custom post-column reference and parameter reference hooks, and stores the function parse information for use by the hooks.

## Parameters / Member Variables
- : ParseState structure to configure with SQL function parsing hooks
- : SQLFunctionParseInfo containing function metadata needed by the parsing hooks

## Dependencies
- Functions called/Symbols referenced:
  - SQLFunctionParseInfoPtr
  - [sql_fn_post_column_ref](sql_fn_post_column_ref.md)
  - [sql_fn_param_ref](sql_fn_param_ref.md)
- Called from (representative examples):
  - [fmgr_sql_validator](../f/fmgr_sql_validator.md) (src/backend/catalog/pg_proc.c:942)
  - [interpret_AS_clause](../i/interpret_AS_clause.md) (src/backend/commands/functioncmds.c:934)
  - [interpret_AS_clause](../i/interpret_AS_clause.md) (src/backend/commands/functioncmds.c:953)
  - [init_sql_fcache](../i/init_sql_fcache.md) (src/backend/executor/functions.c:717)
  - [inline_function](../i/inline_function.md) (src/backend/optimizer/util/clauses.c:4687)
  - [inline_set_returning_function](../i/inline_set_returning_function.md) (src/backend/optimizer/util/clauses.c:5244)

## Notes and Other Information
- The function sets p_pre_columnref_hook to NULL, indicating no preprocessing is needed for column references
- The p_coerce_param_hook is explicitly not used as noted in the comment
- The pinfo parameter is stored in p_ref_hook_state for access by the registered hook functions
- This setup is essential for proper parsing of parameter references ($1, $2, etc.) within SQL function bodies

## Simplified Source

```c
void sql_fn_parser_setup(struct ParseState *pstate, SQLFunctionParseInfoPtr pinfo)
{
    // Configure parser hooks for SQL function context
    pstate->p_pre_columnref_hook = NULL;                    // No preprocessing needed
    pstate->p_post_columnref_hook = sql_fn_post_column_ref; // Handle column references
    pstate->p_paramref_hook = sql_fn_param_ref;             // Handle parameter references ($1, $2, etc.)

    // Store function parse info for use by the hooks
    pstate->p_ref_hook_state = (void *) pinfo;

    // Note: p_coerce_param_hook is not needed for SQL functions
}
```