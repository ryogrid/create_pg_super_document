# dumpTable

## Location
src/bin/pg_dump/pg_dump.c: 15717 - 15856

## Overview
Writes the schema declarations (not data) of a user-defined table, including table definition, ACLs, and column-level ACLs to the archive output.

## Definition


## Detailed Description
This function handles the complete schema dumping process for tables and sequences, coordinating the output of table definitions, access control lists (ACLs), and column-level permissions. It serves as the main dispatcher that determines what components of a table need to be dumped based on the dump configuration and delegates to appropriate specialized functions.

The function operates in several phases: first, it dumps the table definition itself (either as a sequence or regular table schema); then it handles table-level ACLs if they exist; and finally it processes column-level ACLs using prepared statements for efficiency. Column ACLs require special handling because they can exist on system columns and need to be retrieved separately from the main table attributes query.

For column ACLs, the function uses prepared statements that adapt to different PostgreSQL versions, handling the evolution of the privilege system including the addition of initial privileges tracking in version 9.6. Each column ACL is dumped as a dependent object that relies on the table-level ACL.

## Parameters / Member Variables
- : Archive structure for output, containing database connection and dump options
- : TableInfo structure containing complete metadata about the table including columns, ACLs, and relationships

## Dependencies
- Functions called/Symbols referenced:
  - dumpSequence: Dumps sequence definition for RELKIND_SEQUENCE tables
  - dumpTableSchema: Dumps regular table schema definition
  - pg_strdup: Duplicates strings for memory management
  - fmtId: Formats identifiers with proper SQL quoting
  - dumpACL: Dumps access control lists for objects
  - createPQExpBuffer: Creates buffer for SQL query construction
  - appendPQExpBufferStr: Adds SQL text to query buffer
  - ExecuteSqlStatement: Executes prepared statement creation
  - printfPQExpBuffer: Formats parameterized queries
  - ExecuteSqlQuery: Executes column ACL retrieval query
  - PQgetvalue: Extracts values from query results
  - PQclear: Frees query result memory
  - destroyPQExpBuffer: Cleans up query buffers
- Called from:
  - dumpDumpableObject: Called as part of general object dumping dispatcher
  - fmtQualifiedDumpable: Referenced for qualified name formatting

## Notes and Other Information
- Skips execution entirely if dataOnly dump option is set
- Handles both tables and sequences through appropriate delegation
- Uses prepared statements for column ACL queries to improve performance when processing multiple tables
- Column ACL queries adapt to PostgreSQL version differences, particularly around initial privileges introduced in 9.6
- Column ACLs depend on table-level ACLs to ensure correct restoration order
- Hard-codes column default ACL as empty ('{}') to avoid complex owner name resolution
- Memory management includes proper cleanup of duplicated strings
- Part of the schema dumping infrastructure, focusing solely on structure and permissions, not data
- Supports parallel dumping by properly establishing dependencies between related ACL objects