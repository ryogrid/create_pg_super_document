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

## Simplified Source

```c
static void dumpTablespaces(PGconn *conn)
{
    PGresult *res;
    int i;

    // Query all user-defined tablespaces (exclude built-in pg_* tablespaces)
    res = executeQuery(conn, "SELECT oid, spcname, "
                           "pg_catalog.pg_get_userbyid(spcowner) AS spcowner, "
                           "pg_catalog.pg_tablespace_location(oid), "
                           "spcacl, acldefault('t', spcowner) AS acldefault, "
                           "array_to_string(spcoptions, ', '),"
                           "pg_catalog.shobj_description(oid, 'pg_tablespace') "
                           "FROM pg_catalog.pg_tablespace "
                           "WHERE spcname !~ '^pg_' "
                           "ORDER BY 1");

    // Print header if tablespaces found
    if (PQntuples(res) > 0)
        fprintf(OPF, "--\n-- Tablespaces\n--\n\n");

    // Process each tablespace
    for (i = 0; i < PQntuples(res); i++)
    {
        PQExpBuffer buf = createPQExpBuffer();
        Oid spcoid = atooid(PQgetvalue(res, i, 0));
        char *spcname = PQgetvalue(res, i, 1);
        char *spcowner = PQgetvalue(res, i, 2);
        char *spclocation = PQgetvalue(res, i, 3);
        char *spcacl = PQgetvalue(res, i, 4);
        char *acldefault = PQgetvalue(res, i, 5);
        char *spcoptions = PQgetvalue(res, i, 6);
        char *spccomment = PQgetvalue(res, i, 7);
        char *formatted_name = pg_strdup(fmtId(spcname));

        // Handle binary upgrade mode - preserve original OID
        if (binary_upgrade) {
            appendPQExpBuffer(buf, "SELECT pg_catalog.binary_upgrade_set_next_pg_tablespace_oid('%u'::pg_catalog.oid);\n", spcoid);
        }

        // Generate CREATE TABLESPACE statement
        appendPQExpBuffer(buf, "CREATE TABLESPACE %s", formatted_name);
        appendPQExpBuffer(buf, " OWNER %s", fmtId(spcowner));

        // Handle location - empty string for in-place tablespaces
        appendPQExpBufferStr(buf, " LOCATION ");
        if (is_absolute_path(spclocation))
            appendStringLiteralConn(buf, spclocation, conn);
        else
            appendStringLiteralConn(buf, "", conn);
        appendPQExpBufferStr(buf, ";\n");

        // Add tablespace options if present
        if (spcoptions && spcoptions[0] != '\0')
            appendPQExpBuffer(buf, "ALTER TABLESPACE %s SET (%s);\n", formatted_name, spcoptions);

        // Generate ACL commands for permissions
        if (!skip_acls) {
            buildACLCommands(formatted_name, NULL, NULL, "TABLESPACE",
                           spcacl, acldefault, spcowner, "", server_version, buf);
        }

        // Add comment if present
        if (!no_comments && spccomment && spccomment[0] != '\0') {
            appendPQExpBuffer(buf, "COMMENT ON TABLESPACE %s IS ", formatted_name);
            appendStringLiteralConn(buf, spccomment, conn);
            appendPQExpBufferStr(buf, ";\n");
        }

        // Add security labels
        if (!no_security_labels)
            buildShSecLabels(conn, "pg_tablespace", spcoid, "TABLESPACE", spcname, buf);

        // Output the generated SQL
        fprintf(OPF, "%s", buf->data);

        // Cleanup
        free(formatted_name);
        destroyPQExpBuffer(buf);
    }

    PQclear(res);
    fprintf(OPF, "\n\n");
}
```