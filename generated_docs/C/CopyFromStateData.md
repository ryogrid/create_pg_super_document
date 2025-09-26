# CopyFromStateData

## Location
[src/include/commands/copyfrom_internal.h:59-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/commands/copyfrom_internal.h#L59-L181)

## Overview
CopyFromStateData is a comprehensive structure that contains all the state variables used throughout a COPY FROM operation in PostgreSQL, managing everything from input parsing to error handling and data conversion.

## Definition
```c
typedef struct CopyFromStateData
{
    /* low-level state data */
    CopySource    copy_src;         /* type of copy source */
    FILE         *copy_file;        /* used if copy_src == COPY_FILE */
    StringInfo    fe_msgbuf;        /* used if copy_src == COPY_FRONTEND */

    EolType       eol_type;         /* EOL type of input */
    int           file_encoding;    /* file or remote side's character encoding */
    bool          need_transcoding; /* file encoding diff from server? */
    Oid           conversion_proc;  /* encoding conversion function */

    /* parameters from the COPY command */
    Relation      rel;              /* relation to copy from */
    List         *attnumlist;       /* integer list of attnums to copy */
    char         *filename;         /* filename, or NULL for STDIN */
    bool          is_program;       /* is 'filename' a program to popen? */
    copy_data_source_cb data_source_cb; /* function for reading data */

    CopyFormatOptions opts;
    bool         *convert_select_flags; /* per-column CSV/TEXT CS flags */
    Node         *whereClause;      /* WHERE condition (or NULL) */

    /* these are just for error messages, see CopyFromErrorCallback */
    const char   *cur_relname;      /* table name for error messages */
    uint64        cur_lineno;       /* line number for error messages */
    const char   *cur_attname;      /* current att for error messages */
    const char   *cur_attval;       /* current att value for error messages */
    bool          relname_only;     /* don't output line number, att, etc. */

    /* Working state */
    MemoryContext copycontext;      /* per-copy execution context */

    AttrNumber    num_defaults;     /* count of att that are missing and have default value */
    FmgrInfo     *in_functions;     /* array of input functions for each attrs */
    Oid          *typioparams;      /* array of element types for in_functions */
    ErrorSaveContext *escontext;    /* soft error trapper during in_functions execution */
    uint64        num_errors;       /* total number of rows which contained soft errors */
    int          *defmap;           /* array of default att numbers related to missing att */
    ExprState   **defexprs;         /* array of default att expressions for all att */
    bool         *defaults;         /* if DEFAULT marker was found for corresponding att */
    bool          volatile_defexprs; /* is any of defexprs volatile? */
    List         *range_table;      /* single element list of RangeTblEntry */
    List         *rteperminfos;     /* single element list of RTEPermissionInfo */
    ExprState    *qualexpr;

    TransitionCaptureState *transition_capture;

    /* Buffer management for performance optimization */
    StringInfoData attribute_buf;   /* buffer for separated, de-escaped text fields */
    
    int           max_fields;
    char        **raw_fields;       /* field raw data pointers found by COPY FROM */

    StringInfoData line_buf;        /* whole input line being processed */
    bool          line_buf_valid;   /* contains the row being processed? */

    /* Input buffer management */
    char         *input_buf;        /* input data, converted to database encoding */
    int           input_buf_index;  /* next byte to process */
    int           input_buf_len;    /* total # of bytes stored */
    bool          input_reached_eof; /* true if we reached EOF */
    bool          input_reached_error; /* true if a conversion error happened */

    char         *raw_buf;          /* raw input data from source */
    int           raw_buf_index;    /* next byte to process */
    int           raw_buf_len;      /* total # of bytes stored */
    bool          raw_reached_eof;  /* true if we reached EOF */

    uint64        bytes_processed;  /* number of bytes processed so far */
} CopyFromStateData;
```

## Detailed Description
CopyFromStateData is the central state management structure for PostgreSQL's COPY FROM operations. It encapsulates all the necessary information and working state required to efficiently read, parse, and process data from external sources into PostgreSQL tables. The structure is designed with performance optimization in mind, using buffer reuse strategies to minimize memory allocation overhead during bulk data operations.

The structure handles multiple input sources (files, programs, frontend connections), different data formats (text, CSV, binary), character encoding conversions, error handling with soft error support, and WHERE clause filtering. It also manages default value processing, trigger state capture, and maintains detailed error reporting context.

## Parameters / Member Variables

### Low-level State Data
- `copy_src`: Specifies the type of copy source (file, frontend, etc.)
- `copy_file`: File pointer when copying from a file
- `fe_msgbuf`: Message buffer for frontend connections
- `eol_type`: End-of-line type of the input data
- `file_encoding`: Character encoding of the source file or remote connection
- `need_transcoding`: Boolean indicating if encoding conversion is required
- `conversion_proc`: OID of the encoding conversion function

### COPY Command Parameters
- `rel`: The target relation for the COPY FROM operation
- `attnumlist`: List of attribute numbers to copy (subset of columns)
- `filename`: Source filename, or NULL for STDIN
- `is_program`: Boolean indicating if filename is a program to execute
- `data_source_cb`: Callback function for reading data from custom sources
- `opts`: Copy format options (delimiter, quote character, etc.)
- `convert_select_flags`: Per-column conversion flags for CSV/TEXT formats
- `whereClause`: WHERE condition for filtering rows during copy

### Error Reporting Context
- `cur_relname`: Current table name for error messages
- `cur_lineno`: Current line number for error reporting
- `cur_attname`: Current attribute name being processed
- `cur_attval`: Current attribute value for error context
- `relname_only`: Flag to suppress detailed error location info

### Working State Management
- `copycontext`: Memory context for the copy operation
- `num_defaults`: Count of attributes with default values
- `in_functions`: Array of input functions for data conversion
- `typioparams`: Array of type parameters for input functions
- `escontext`: Error context for soft error handling
- `num_errors`: Total count of rows with soft errors
- `defmap`: Mapping of default attribute numbers
- `defexprs`: Array of default value expressions
- `defaults`: Boolean array indicating DEFAULT markers found
- `volatile_defexprs`: Flag indicating if any default expressions are volatile
- `range_table`: Single-element list containing the RangeTblEntry
- `rteperminfos`: Permission information for the range table
- `qualexpr`: Compiled WHERE clause expression
- `transition_capture`: State for capturing transition tuples

### Buffer Management
- `attribute_buf`: Reusable buffer for field data to avoid allocation overhead
- `max_fields`: Maximum number of fields expected
- `raw_fields`: Array of pointers to raw field data
- `line_buf`: Buffer containing the current input line
- `line_buf_valid`: Flag indicating if line_buf contains valid data
- `input_buf`: Buffer for database-encoded input data
- `input_buf_index`: Current position in input buffer
- `input_buf_len`: Total bytes stored in input buffer
- `input_reached_eof`: EOF flag for input buffer
- `input_reached_error`: Error flag for input conversion
- `raw_buf`: Buffer for raw input data before encoding conversion
- `raw_buf_index`: Current position in raw buffer
- `raw_buf_len`: Total bytes in raw buffer
- `raw_reached_eof`: EOF flag for raw buffer
- `bytes_processed`: Total number of bytes processed

## Dependencies
- Functions called/Symbols referenced:
  - CopySource
  - EolType
  - [CopyFormatOptions](CopyFormatOptions.md)
  - [ErrorSaveContext](../E/ErrorSaveContext.md)
  - [TransitionCaptureState](../T/TransitionCaptureState.md)
  - copy_data_source_cb
- Called from (representative examples):
  - [BeginCopyFrom](../B/BeginCopyFrom.md)
  - [CopyFromState](CopyFromState.md) (typedef)

## Notes and Other Information
- The structure uses buffer reuse strategies with `INPUT_BUF_SIZE` (65536) and `RAW_BUF_SIZE` (65536) constants to optimize memory allocation during bulk operations
- Two convenience macros `INPUT_BUF_BYTES(cstate)` and `RAW_BUF_BYTES(cstate)` calculate remaining bytes in respective buffers
- The dual buffer system (`input_buf` and `raw_buf`) allows for efficient character encoding conversion while maintaining the original data for error reporting
- Soft error handling through `escontext` allows the operation to continue despite individual row errors, collecting them for later reporting
- The structure is defined in `src/include/commands/copyfrom_internal.h` lines 59-181
- Buffer management is critical for performance - the `attribute_buf` and `line_buf` are reused across rows to minimize palloc/pfree overhead