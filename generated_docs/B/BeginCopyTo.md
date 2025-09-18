# BeginCopyTo

## Location
src/backend/commands/copyto.c: 350 - 725

## Overview
BeginCopyTo initializes and sets up a CopyToState structure to read tuples from a table or query for COPY TO operations, handling all necessary validations and configuration for the copy process.

## Definition
```c
CopyToState BeginCopyTo(ParseState *pstate,
                       Relation rel,
                       RawStmt *raw_query,
                       Oid queryRelId,
                       const char *filename,
                       bool is_program,
                       copy_data_dest_cb data_dest_cb,
                       List *attnamelist,
                       List *options)
```

## Detailed Description
BeginCopyTo performs comprehensive setup for COPY TO operations by creating a CopyToState structure with all necessary configurations. The function validates input relations, processes query statements, handles encoding settings, sets up output destinations (file, program, or callback), and configures attribute lists and formatting options. It creates a dedicated memory context for the copy operation to prevent memory leaks during repeated use. The function also handles Row Level Security (RLS) by converting relation-based copies to query-based copies when necessary, and initializes progress reporting for the copy operation.

## Parameters / Member Variables
- `pstate`: ParseState context for the copy operation
- `rel`: Source relation to copy from (NULL for query-based copy)
- `raw_query`: Raw query statement whose results should be copied (NULL for relation-based copy)
- `queryRelId`: OID of base relation for RLS conversion (InvalidOid if not applicable)
- `filename`: Name of server-local file to write to (NULL for STDOUT)
- `is_program`: True if filename specifies a program to execute rather than a file
- `data_dest_cb`: Callback function to process output data (NULL if not using callback)
- `attnamelist`: List of column names to include in copy (NIL selects all columns)
- `options`: List of DefElem structures containing copy options

## Dependencies
- Functions called/Symbols referenced:
  - ProcessCopyOptions
  - CopyGetAttnums
  - pg_analyze_and_rewrite_fixedparams
  - pg_plan_query
  - ExecutorStart
  - CreateQueryDesc
  - AllocSetContextCreate
  - pgstat_progress_start_command
- Called from (representative examples):
  - DoCopy
  - test_copy_to_callback

## Notes and Other Information
The function performs extensive validation on relation types, rejecting views, materialized views, foreign tables, sequences, and partitioned tables with helpful error messages suggesting alternatives. It handles encoding conversion setup and creates appropriate output destinations based on the parameters. The function integrates with PostgreSQL's progress reporting system and properly manages memory contexts to avoid leaks. When dealing with RLS-enabled relations, it automatically converts simple relation copies to query-based copies to allow proper security clause application.