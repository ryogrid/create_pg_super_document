# CopyToStateData

## Location
[src/backend/commands/copyto.c:65-95](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyto.c#L65-L95)

## Overview
CopyToStateData is a structure that contains all the state variables used throughout a COPY TO operation in PostgreSQL, managing the complete context for exporting data from tables or queries to external destinations.

## Definition
```c
typedef struct CopyToStateData
{
    /* low-level state data */
    CopyDest    copy_dest;          /* type of copy source/destination */
    FILE       *copy_file;          /* used if copy_dest == COPY_FILE */
    StringInfo  fe_msgbuf;          /* used for all dests during COPY TO */

    int         file_encoding;      /* file or remote side's character encoding */
    bool        need_transcoding;   /* file encoding diff from server? */
    bool        encoding_embeds_ascii; /* ASCII can be non-first byte? */

    /* parameters from the COPY command */
    Relation    rel;                /* relation to copy to */
    QueryDesc  *queryDesc;          /* executable query to copy from */
    List       *attnumlist;         /* integer list of attnums to copy */
    char       *filename;           /* filename, or NULL for STDOUT */
    bool        is_program;         /* is 'filename' a program to popen? */
    copy_data_dest_cb data_dest_cb; /* function for writing data */

    CopyFormatOptions opts;
    Node       *whereClause;        /* WHERE condition (or NULL) */

    /*
     * Working state
     */
    MemoryContext copycontext;      /* per-copy execution context */

    FmgrInfo   *out_functions;      /* lookup info for output functions */
    MemoryContext rowcontext;       /* per-row evaluation context */
    uint64      bytes_processed;    /* number of bytes processed so far */
} CopyToStateData;
```

## Detailed Description
CopyToStateData serves as the comprehensive state container for PostgreSQL's COPY TO operations, which export data from database tables or query results to external files, programs, or client connections. The structure is carefully designed to handle various encoding scenarios, particularly multi-byte character encodings where ASCII characters might appear as non-first bytes within multi-byte sequences.

The structure manages both high-level operational parameters (like the source relation, destination file, and format options) and low-level execution state (like encoding conversion requirements, output functions, and memory contexts). It supports different copy destinations and handles the complexities of character encoding conversion when the client and server encodings differ.

## Parameters / Member Variables
- `copy_dest`: Specifies the type of copy destination (file, program, or client connection)
- `copy_file`: File pointer when copying to a file (used when copy_dest == COPY_FILE)
- `fe_msgbuf`: String buffer used for formatting output data for all destination types during COPY TO operations
- `file_encoding`: Character encoding used by the destination file or remote client
- `need_transcoding`: Boolean flag indicating whether character encoding conversion is required
- `encoding_embeds_ascii`: Flag indicating whether the encoding allows ASCII characters to appear as non-first bytes in multi-byte sequences
- `rel`: The source relation (table) from which data is being copied
- `queryDesc`: Executable query descriptor when copying from a query result instead of a table
- `attnumlist`: List of attribute numbers specifying which columns to include in the copy operation
- `filename`: Target filename for file destinations, or NULL for STDOUT
- `is_program`: Boolean indicating whether the filename refers to a program to be executed via popen()
- `data_dest_cb`: Callback function responsible for writing the formatted data to the destination
- `opts`: Structure containing COPY command format options (delimiter, quote character, escape character, etc.)
- `whereClause`: Optional WHERE clause for filtering rows during the copy operation
- `copycontext`: Memory context used for the entire copy operation's allocations
- `out_functions`: Array of function manager info structures for the output functions of each column
- `rowcontext`: Memory context that is reset after processing each row to prevent memory leaks
- `bytes_processed`: Running counter of the total number of bytes that have been processed and sent to the destination

## Dependencies
- Functions called/Symbols referenced:
  - CopyDest
  - [CopyFormatOptions](CopyFormatOptions.md)
  - [QueryDesc](../Q/QueryDesc.md)
- Called from (representative examples):
  - [BeginCopyTo](../B/BeginCopyTo.md)
  - [CopyToState](CopyToState.md)

## Notes and Other Information
The structure includes sophisticated handling for multi-byte character encodings, with special consideration for encodings where ASCII characters might appear as trailing bytes within multi-byte sequences. The `encoding_embeds_ascii` flag determines whether the system needs to use the full `pg_encoding_mblen()` machinery to safely scan data, or whether it can use faster byte-by-byte comparisons.

The dual memory context approach (`copycontext` for operation-wide allocations and `rowcontext` for per-row temporary allocations) is a common PostgreSQL pattern for efficient memory management during bulk operations.