# dumpTablespaces

## Location
[src/bin/pg_dump/pg_dumpall.c:1335-1438](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dumpall.c#L1335-L1438)

## Overview
Generates CREATE TABLESPACE statements to recreate all user-defined tablespaces in a PostgreSQL cluster, including their ownership, permissions, options, comments, and security labels.

## Definition
static void dumpTablespaces(PGconn *conn)

## Detailed Description
This function is a comprehensive tablespace dumping utility within pg_dumpall that handles the complete recreation of user-defined tablespaces. It queries the pg_tablespace system catalog to retrieve detailed information about each tablespace, including metadata such as ownership, location, access control lists, options, comments, and security labels.

The function handles several special cases: binary upgrade scenarios where original OIDs must be preserved, in-place tablespaces that use relative paths (dumped with empty location strings), and proper escaping of string literals in SQL output. It generates CREATE TABLESPACE statements along with any necessary ALTER TABLESPACE commands for options, GRANT/REVOKE commands for permissions, COMMENT statements, and security label assignments.

Like other dump functions, it excludes built-in system tablespaces (those with pg_ prefix) and only processes user-defined tablespaces to ensure clean restoration.

## Parameters / Member Variables
- conn: PostgreSQL connection handle used to execute queries against the database

## Dependencies
- Functions called/Symbols referenced:
  - [executeQuery](../e/executeQuery.md): Executes SQL query to retrieve comprehensive tablespace information
  - atooid: Converts string representation to OID type
  - [fmtId](../f/fmtId.md): Formats identifiers for safe SQL output
  - is_absolute_path: Checks if tablespace location uses absolute path
  - [appendStringLiteralConn](../a/appendStringLiteralConn.md): Safely appends string literals to SQL buffer
  - [buildACLCommands](../b/buildACLCommands.md): Generates GRANT/REVOKE commands from ACL data
  - [buildShSecLabels](../b/buildShSecLabels.md): Generates security label assignments
  - [PQfinish](../P/PQfinish.md): Closes database connection on error
  - [exit_nicely](../e/exit_nicely.md): Performs clean exit with error status
- Called from (representative examples):
  - [main](../m/main.md): Primary entry point in pg_dumpall utility for tablespace dumping

## Notes and Other Information
- Supports binary upgrade mode where original tablespace OIDs are preserved
- Handles in-place tablespaces by dumping them with empty location strings
- Respects global flags: skip_acls, no_comments, no_security_labels
- Generates comprehensive SQL including CREATE, ALTER, GRANT, COMMENT, and security label statements
- Special handling for tablespace options through ALTER TABLESPACE SET commands
- Error handling includes proper cleanup and informative error messages
- Essential component of complete cluster backup and restoration process