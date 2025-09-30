# BeginCopyFrom

## Location
[src/backend/commands/copyfrom.c:1368-1786](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfrom.c#L1368-L1786)

## Overview
Initializes and configures a CopyFromState structure for COPY FROM operations, setting up input parsing, data conversion, encoding handling, and all necessary metadata for reading tuples from various data sources.

## Definition
```c
CopyFromState
BeginCopyFrom(ParseState *pstate,
              Relation rel,
              Node *whereClause,
              const char *filename,
              bool is_program,
              copy_data_source_cb data_source_cb,
              List *attnamelist,
              List *options)
```

## Detailed Description
BeginCopyFrom serves as the initialization function for COPY FROM operations, responsible for setting up all the necessary state, buffers, and configuration before data can be read and processed. It creates a dedicated memory context for the COPY operation, processes all command options, sets up input/output functions for data type conversion, configures encoding transcoding if needed, and opens the appropriate data source (file, pipe, program, or callback).

The function handles complex setup tasks including validating column specifications, setting up FORCE_NOT_NULL and FORCE_NULL column flags, configuring default value expressions, setting up encoding conversion routines, and initializing progress reporting. It supports various input sources including files, standard input, executed programs, and custom callback functions.

## Parameters / Member Variables
- `pstate`: ParseState from the parser, containing parse-time information and range table entries
- `rel`: Target relation where data will be inserted, used as template for tuple structure
- `whereClause`: Optional WHERE clause node for filtering input rows during COPY
- `filename`: Path to the input file, NULL indicates reading from standard input
- `is_program`: True if filename represents a program to execute rather than a regular file
- `data_source_cb`: Optional callback function for providing input data programmatically
- `attnamelist`: List of column names to process, NIL means all columns
- `options`: List of DefElem nodes containing COPY command options (format, delimiter, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [ProcessCopyOptions](../P/ProcessCopyOptions.md)
  - [CopyGetAttnums](../C/CopyGetAttnums.md)
  - makeNode (ErrorSaveContext)
  - [pg_get_client_encoding](../p/pg_get_client_encoding.md)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - [FindDefaultConversionProc](../F/FindDefaultConversionProc.md)
  - [getTypeBinaryInputInfo](../g/getTypeBinaryInputInfo.md)
  - [getTypeInputInfo](../g/getTypeInputInfo.md)
  - [fmgr_info](../f/fmgr_info.md)
  - [build_column_default](../b/build_column_default.md)
  - [expression_planner](../e/expression_planner.md)
  - [ExecInitExpr](../E/ExecInitExpr.md)
  - [contain_volatile_functions_not_nextval](../c/contain_volatile_functions_not_nextval.md)
  - [pgstat_progress_start_command](../p/pgstat_progress_start_command.md)
  - [ReceiveCopyBegin](../R/ReceiveCopyBegin.md)
  - [OpenPipeStream](../O/OpenPipeStream.md)
  - [AllocateFile](../A/AllocateFile.md)
  - [ReceiveCopyBinaryHeader](../R/ReceiveCopyBinaryHeader.md)
  - AllocSetContextCreate
- Called from (representative examples):
  - [DoCopy](../D/DoCopy.md) (main COPY command processing)
  - [copy_table](../c/copy_table.md) (logical replication)

## Notes and Other Information
- Creates a dedicated "COPY" memory context to avoid leaks during repeated use
- Supports both text and binary format processing with different buffer allocation strategies
- Handles encoding conversion automatically when file encoding differs from database encoding
- Sets up soft error handling for ON_ERROR modes to allow continued processing after errors
- Initializes progress reporting framework for monitoring COPY operation status
- Validates that FORCE_NOT_NULL and FORCE_NULL columns are actually referenced in the COPY
- Detects volatile default expressions that would prevent multi-insert optimization
- Supports multiple input sources: files, stdin, executed programs, and callback functions
- Allocates and configures input buffers based on whether encoding conversion is needed
- Binary format requires reading and validating a binary header before processing data

## Simplified Source

```c
CopyFromState BeginCopyFrom(ParseState *pstate,
                           Relation rel,
                           Node *whereClause,
                           const char *filename,
                           bool is_program,
                           copy_data_source_cb data_source_cb,
                           List *attnamelist,
                           List *options) {
    CopyFromState cstate;
    TupleDesc tupDesc;

    // Allocate and initialize copy state structure
    cstate = palloc0(sizeof(CopyFromStateData));
    cstate->copycontext = AllocSetContextCreate(CurrentMemoryContext, "COPY",
                                                ALLOCSET_DEFAULT_SIZES);

    // Process COPY command options (format, delimiter, etc.)
    ProcessCopyOptions(pstate, &cstate->opts, true, options);

    // Set up target relation and column mapping
    cstate->rel = rel;
    tupDesc = RelationGetDescr(cstate->rel);
    cstate->attnumlist = CopyGetAttnums(tupDesc, cstate->rel, attnamelist);

    // Set up FORCE_NOT_NULL and FORCE_NULL column flags
    setup_force_flags(cstate, tupDesc);

    // Configure encoding conversion if needed
    setup_encoding_conversion(cstate);

    // Set up error handling context for ON_ERROR modes
    if (cstate->opts.on_error != COPY_ON_ERROR_STOP) {
        cstate->escontext = makeNode(ErrorSaveContext);
        cstate->escontext->error_occurred = false;
    }

    // Initialize input/output functions for data type conversion
    setup_io_functions(cstate, tupDesc);

    // Set up default value expressions for missing columns
    setup_default_expressions(cstate, tupDesc);

    // Allocate input buffers based on text/binary mode
    allocate_input_buffers(cstate);

    // Configure data source (file, stdin, program, or callback)
    setup_data_source(cstate, filename, is_program, data_source_cb);

    // Initialize progress reporting
    pgstat_progress_start_command(PROGRESS_COMMAND_COPY, RelationGetRelid(rel));

    // For binary format, read and validate header
    if (cstate->opts.binary) {
        ReceiveCopyBinaryHeader(cstate);
    }

    // Set up workspace for field parsing
    if (!cstate->opts.binary) {
        AttrNumber attr_count = list_length(cstate->attnumlist);
        cstate->max_fields = attr_count;
        cstate->raw_fields = palloc(attr_count * sizeof(char *));
    }

    return cstate;
}
```