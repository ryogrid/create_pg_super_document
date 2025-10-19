# gen_reindex_command

## Location
[src/bin/scripts/reindexdb.c:506-588](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/scripts/reindexdb.c#L506-L588)

## Overview
Generates and appends a SQL REINDEX command to a buffer based on specified reindex type and options.

## Definition

```c
static void
gen_reindex_command(PGconn *conn, ReindexType type, const char *name,
					bool echo, bool verbose, bool concurrently,
					const char *tablespace, PQExpBufferData *sql)
```
## Detailed Description
This function constructs a properly formatted SQL REINDEX command by appending to an existing PQExpBuffer. It handles the complex syntax variations of the REINDEX command across different PostgreSQL versions and object types.

The function builds commands in the format:


**Options handling**:
- VERBOSE: Added when verbose flag is true
- TABLESPACE: Added when tablespace parameter is provided (PostgreSQL 14+)
- Uses parenthesized syntax for options when needed

**Object type support**:
- DATABASE: Reindex entire database
- SYSTEM: Reindex system catalogs
- SCHEMA: Reindex all objects in schema
- TABLE: Reindex specific table
- INDEX: Reindex specific index

**Name formatting**:
- Database/System names: Uses proper identifier encoding
- Table/Index names: Uses qualified relation format (schema.object)
- Schema names: Uses plain name format

## Parameters / Member Variables
- `*conn`: PostgreSQL database connection for encoding context
- `type`: Type of reindex operation (REINDEX_DATABASE, REINDEX_SYSTEM, REINDEX_SCHEMA, REINDEX_TABLE, REINDEX_INDEX)
- `*name`: Name of the database object to reindex
- `echo`: Whether to echo the command (affects name qualification)
- `verbose`: Whether to include VERBOSE option in the command
- `concurrently`: Whether to include CONCURRENTLY option
- `*tablespace`: Target tablespace name for rebuilt indexes (optional, PostgreSQL 14+)
- `*sql`: Output buffer to append the generated command to
## Dependencies
- Functions called/Symbols referenced:
  - [appendPQExpBufferStr](../a/appendPQExpBufferStr.md)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md)
  - [appendPQExpBufferChar](../a/appendPQExpBufferChar.md)
  - [fmtIdEnc](../f/fmtIdEnc.md)
  - [PQclientEncoding](../P/PQclientEncoding.md)
  - [appendQualifiedRelation](../a/appendQualifiedRelation.md)
  - ReindexType enums (REINDEX_DATABASE, REINDEX_SYSTEM, REINDEX_SCHEMA, REINDEX_TABLE, REINDEX_INDEX)
- Called from (representative examples):
  - [reindex_one_database](../r/reindex_one_database.md) (reindexdb.c:452, 461, 468)

## Notes and Other Information
- The function uses version-aware syntax construction, handling differences between PostgreSQL versions
- Parenthesized option syntax is only supported for CONCURRENTLY since PostgreSQL 14
- CONCURRENTLY option can be placed after object type since PostgreSQL 12
- Proper identifier encoding is used to handle special characters and reserved words in object names
- The function always terminates the command with a semicolon
- No validation is performed on input parameters - caller is responsible for providing valid values
- The generated command is appended to existing buffer content, allowing for command batching

## Simplified Source

```c
static void
gen_reindex_command(PGconn *conn, ReindexType type, const char *name,
                    bool echo, bool verbose, bool concurrently,
                    const char *tablespace, PQExpBufferData *sql)
{
    const char *sep = "(";

    // Start building REINDEX command
    appendPQExpBufferStr(sql, "REINDEX ");

    // Add options in parentheses if any are specified
    if (verbose) {
        appendPQExpBuffer(sql, "%sVERBOSE", sep);
        sep = ", ";
    }

    if (tablespace) {
        appendPQExpBuffer(sql, "%sTABLESPACE %s", sep,
                         fmtIdEnc(tablespace, PQclientEncoding(conn)));
        sep = ", ";
    }

    // Close options parentheses if any options were added
    if (sep != "(")
        appendPQExpBufferStr(sql, ") ");

    // Add object type keyword
    switch (type) {
        case REINDEX_DATABASE:
            appendPQExpBufferStr(sql, "DATABASE ");
            break;
        case REINDEX_INDEX:
            appendPQExpBufferStr(sql, "INDEX ");
            break;
        case REINDEX_SCHEMA:
            appendPQExpBufferStr(sql, "SCHEMA ");
            break;
        case REINDEX_SYSTEM:
            appendPQExpBufferStr(sql, "SYSTEM ");
            break;
        case REINDEX_TABLE:
            appendPQExpBufferStr(sql, "TABLE ");
            break;
    }

    // Add CONCURRENTLY option after object type
    if (concurrently)
        appendPQExpBufferStr(sql, "CONCURRENTLY ");

    // Add object name with appropriate formatting
    switch (type) {
        case REINDEX_DATABASE:
        case REINDEX_SYSTEM:
            // Simple identifier encoding for database/system names
            appendPQExpBufferStr(sql, fmtIdEnc(name, PQclientEncoding(conn)));
            break;
        case REINDEX_INDEX:
        case REINDEX_TABLE:
            // Qualified relation format for tables and indexes
            appendQualifiedRelation(sql, name, conn, echo);
            break;
        case REINDEX_SCHEMA:
            // Plain name for schemas
            appendPQExpBufferStr(sql, name);
            break;
    }

    // Terminate command with semicolon
    appendPQExpBufferChar(sql, ';');
}
```