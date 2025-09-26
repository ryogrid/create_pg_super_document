# CopyStmt

## Location
[src/include/nodes/parsenodes.h:2586-2599](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/parsenodes.h#L2586-L2599)

## Overview
CopyStmt is a parse tree node structure that represents SQL COPY statements, supporting data import/export operations between PostgreSQL tables and files or programs.

## Definition
```c
typedef struct CopyStmt
{
    NodeTag    type;
    RangeVar  *relation;        /* the relation to copy */
    Node      *query;           /* the query (SELECT or DML statement with
                                 * RETURNING) to copy, as a raw parse tree */
    List      *attlist;         /* List of column names (as Strings), or NIL
                                 * for all columns */
    bool       is_from;         /* TO or FROM */
    bool       is_program;      /* is 'filename' a program to popen? */
    char      *filename;        /* filename, or NULL for STDIN/STDOUT */
    List      *options;         /* List of DefElem nodes */
    Node      *whereClause;     /* WHERE condition (or NULL) */
} CopyStmt;
```

## Detailed Description
CopyStmt represents the parsed form of PostgreSQL COPY statements, which provide high-performance bulk data transfer capabilities. The structure supports three main variants:
1. COPY relation FROM file - importing data from a file into a table
2. COPY relation TO file - exporting table data to a file  
3. COPY (query) TO file - exporting query results to a file

The statement supports flexible data sources including regular files, programs (via popen), and standard input/output streams. Various format options can be specified through the options list, and WHERE clauses can filter the data being copied.

## Parameters / Member Variables
- `type`: NodeTag identifying this as a CopyStmt node in the parse tree
- `relation`: RangeVar specifying the table to copy from/to (mutually exclusive with query)
- `query`: Raw parse tree node for SELECT or DML statement with RETURNING clause (mutually exclusive with relation)
- `attlist`: List of String nodes specifying column names to copy, or NIL for all columns
- `is_from`: Boolean flag indicating direction (true for FROM file to table, false for TO file from table/query)
- `is_program`: Boolean flag indicating whether filename refers to a program to execute via popen
- `filename`: String specifying the file path, or NULL to use STDIN/STDOUT
- `options`: List of DefElem nodes specifying format options (delimiter, format, header, etc.)
- `whereClause`: Optional WHERE condition node for filtering copied data

## Dependencies
- Functions called/Symbols referenced:
  - NodeTag (parse tree node identification)
  - RangeVar (table reference structure)
  - Node (generic parse tree node)
  - List (PostgreSQL list data structure)
  - DefElem (definition element for options)

- Called from (representative examples):
  - DoCopy
  - init_execution_state
  - _SPI_execute_plan
  - ClassifyUtilityCommandAsReadOnly
  - standard_ProcessUtility
  - GetCommandLogLevel

## Notes and Other Information
- Exactly one of relation and query must be non-NULL in any given CopyStmt
- The is_program flag allows COPY to interface with external programs for data processing pipelines
- WHERE clauses are supported for filtering data during export operations
- The structure supports both text and binary formats through the options mechanism
- COPY operations are highly optimized for bulk data transfer and bypass much of the normal SQL processing overhead
- Security considerations apply when using the program option, which may be restricted in some configurations