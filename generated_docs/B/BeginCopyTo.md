# BeginCopyTo

## Location
[src/backend/commands/copyto.c:350-725](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyto.c#L350-L725)

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
  - [ProcessCopyOptions](../P/ProcessCopyOptions.md)
  - [CopyGetAttnums](../C/CopyGetAttnums.md)
  - [pg_analyze_and_rewrite_fixedparams](../p/pg_analyze_and_rewrite_fixedparams.md)
  - [pg_plan_query](../p/pg_plan_query.md)
  - [ExecutorStart](../E/ExecutorStart.md)
  - [CreateQueryDesc](../C/CreateQueryDesc.md)
  - AllocSetContextCreate
  - [pgstat_progress_start_command](../p/pgstat_progress_start_command.md)
- Called from (representative examples):
  - [DoCopy](../D/DoCopy.md)
  - [test_copy_to_callback](../t/test_copy_to_callback.md)

## Notes and Other Information
The function performs extensive validation on relation types, rejecting views, materialized views, foreign tables, sequences, and partitioned tables with helpful error messages suggesting alternatives. It handles encoding conversion setup and creates appropriate output destinations based on the parameters. The function integrates with PostgreSQL's progress reporting system and properly manages memory contexts to avoid leaks. When dealing with RLS-enabled relations, it automatically converts simple relation copies to query-based copies to allow proper security clause application.

## Simplified Source

```c
CopyToState BeginCopyTo(ParseState *pstate,
                       Relation rel,
                       RawStmt *raw_query,
                       Oid queryRelId,
                       const char *filename,
                       bool is_program,
                       copy_data_dest_cb data_dest_cb,
                       List *attnamelist,
                       List *options) {
    CopyToState cstate;
    TupleDesc tupDesc;
    bool pipe = (filename == NULL && data_dest_cb == NULL);

    // Validate relation type for direct table copies
    if (rel != NULL && rel->rd_rel->relkind != RELKIND_RELATION) {
        // Report appropriate error for unsupported relation types
        // (views, materialized views, foreign tables, sequences, etc.)
        report_unsupported_relation_error(rel);
    }

    // Allocate and initialize copy state structure
    cstate = palloc0(sizeof(CopyToStateData));
    cstate->copycontext = AllocSetContextCreate(CurrentMemoryContext, "COPY",
                                                ALLOCSET_DEFAULT_SIZES);

    // Process COPY command options
    ProcessCopyOptions(pstate, &cstate->opts, false, options);

    // Set up data source (relation or query)
    if (rel) {
        // Direct table copy
        cstate->rel = rel;
        tupDesc = RelationGetDescr(cstate->rel);
    } else {
        // Query-based copy
        cstate->rel = NULL;

        // Parse, analyze, and rewrite the query
        List *rewritten = pg_analyze_and_rewrite_fixedparams(raw_query,
                                                             pstate->p_sourcetext,
                                                             NULL, 0, NULL);

        // Validate rewritten query
        validate_copy_query(rewritten);

        Query *query = linitial_node(Query, rewritten);

        // Plan and prepare the query for execution
        PlannedStmt *plan = pg_plan_query(query, pstate->p_sourcetext,
                                          CURSOR_OPT_PARALLEL_OK, NULL);

        // Set up executor and get tuple descriptor
        setup_query_executor(cstate, plan, pstate);
        tupDesc = cstate->queryDesc->tupDesc;
    }

    // Set up column mapping and attributes
    cstate->attnumlist = CopyGetAttnums(tupDesc, cstate->rel, attnamelist);

    // Configure FORCE_QUOTE column flags
    setup_force_quote_flags(cstate, tupDesc);

    // Set up encoding conversion
    setup_encoding_conversion(cstate);

    // Configure output destination (file, pipe, program, or callback)
    setup_output_destination(cstate, filename, is_program, data_dest_cb, pipe);

    // Initialize progress reporting
    pgstat_progress_start_command(PROGRESS_COMMAND_COPY,
                                  cstate->rel ? RelationGetRelid(cstate->rel) : InvalidOid);

    return cstate;
}
```