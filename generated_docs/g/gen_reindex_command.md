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
- : PostgreSQL database connection for encoding context
- : Type of reindex operation (REINDEX_DATABASE, REINDEX_SYSTEM, REINDEX_SCHEMA, REINDEX_TABLE, REINDEX_INDEX)
- : Name of the database object to reindex
- : Whether to echo the command (affects name qualification)
- : Whether to include VERBOSE option in the command
- : Whether to include CONCURRENTLY option
- : Target tablespace name for rebuilt indexes (optional, PostgreSQL 14+)
- : Output buffer to append the generated command to

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