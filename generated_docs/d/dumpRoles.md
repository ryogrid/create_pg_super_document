# dumpRoles

## Location
[src/bin/pg_dump/pg_dumpall.c:787-994](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dumpall.c#L787-L994)

## Overview
The dumpRoles function generates SQL CREATE ROLE and ALTER ROLE statements for all non-system roles in a PostgreSQL database, preserving role properties, passwords, comments, and security labels.

## Definition

```c
static void
dumpRoles(PGconn *conn)
```
## Detailed Description
The dumpRoles function is a core component of PostgreSQL's pg_dumpall utility that extracts role definitions from system catalogs and generates corresponding SQL statements for database cluster restoration. It handles version-specific differences in PostgreSQL's role system, particularly the introduction of the  (Row Level Security bypass) attribute in version 9.5.

The function constructs comprehensive CREATE ROLE and ALTER ROLE statements with all role attributes including superuser status, inheritance rights, database/role creation privileges, login capability, replication rights, connection limits, passwords, validity periods, and comments. For binary upgrades, it preserves the original OIDs to maintain system consistency.

The function processes roles in two phases: first dumping role definitions, then dumping user configuration settings separately to handle potential cross-references between roles.

## Parameters / Member Variables
- `*conn`: PostgreSQL database connection handle used to query system catalogs
## Dependencies
- Functions called/Symbols referenced:
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (format SQL query strings for different PostgreSQL versions)
  - [executeQuery](../e/executeQuery.md) (execute SQL queries against the database)
  - atooid (convert string OID to numeric OID type)
  - pg_log_warning (log warning messages for skipped system roles)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md), appendPQExpBuffer, appendPQExpBufferStr (manage query buffer)
  - [fmtId](../f/fmtId.md) (format SQL identifiers with proper quoting)
  - [PQgetisnull](../P/PQgetisnull.md), PQgetvalue (check for NULL values and retrieve result data)
  - [appendStringLiteralConn](../a/appendStringLiteralConn.md) (safely append string literals to SQL)
  - [buildShSecLabels](../b/buildShSecLabels.md) (generate security label statements)
  - [dumpUserConfig](dumpUserConfig.md) (dump role-specific configuration parameters)
  - [createPQExpBuffer](../c/createPQExpBuffer.md), destroyPQExpBuffer (manage query buffers)
- Called from:
  - [main](../m/main.md) (in src/bin/pg_dump/pg_dumpall.c as part of the cluster dump process)

## Notes and Other Information
- Function is marked as , indicating it's only used within pg_dumpall.c
- Uses global variables: , , , , , , 
- Handles PostgreSQL version differences: 9.6+ excludes system roles, 9.5+ includes , earlier versions set it to false
- Skips roles starting with 'pg_' to avoid system roles, with warning messages
- Uses CREATE ROLE + ALTER ROLE pattern to handle existing roles gracefully
- For binary upgrades, preserves original OIDs except for the current user role
- Dumps role configurations separately after all roles to handle cross-references
- Orders roles alphabetically by name (ORDER BY 2) for consistent output
- Includes comprehensive role attribute handling: SUPERUSER, INHERIT, CREATEROLE, CREATEDB, LOGIN, REPLICATION, BYPASSRLS, CONNECTION LIMIT, PASSWORD, VALID UNTIL

## Simplified Source

```c
static void dumpRoles(PGconn *conn) {
    PQExpBuffer buf = createPQExpBuffer();
    PGresult *res;
    int i_oid, i_rolname, i_rolsuper, i_rolinherit, i_rolcreaterole,
        i_rolcreatedb, i_rolcanlogin, i_rolconnlimit, i_rolpassword,
        i_rolvaliduntil, i_rolreplication, i_rolbypassrls, i_rolcomment,
        i_is_current_user;

    // Build query based on server version (9.6+ excludes system roles)
    if (server_version >= 90600) {
        printfPQExpBuffer(buf,
            "SELECT oid, rolname, rolsuper, rolinherit, rolcreaterole, "
            "rolcreatedb, rolcanlogin, rolconnlimit, rolpassword, "
            "rolvaliduntil, rolreplication, rolbypassrls, "
            "pg_catalog.shobj_description(oid, 'pg_authid') as rolcomment, "
            "rolname = current_user AS is_current_user "
            "FROM %s WHERE rolname !~ '^pg_' ORDER BY 2", role_catalog);
    } else {
        // Similar query for older versions without rolbypassrls
        printfPQExpBuffer(buf, "SELECT ... false as rolbypassrls ...");
    }

    res = executeQuery(conn, buf->data);

    // Get column indices for all role attributes
    i_oid = PQfnumber(res, "oid");
    i_rolname = PQfnumber(res, "rolname");
    // ... (additional column indices)

    if (PQntuples(res) > 0) {
        fprintf(OPF, "--\n-- Roles\n--\n\n");
    }

    // Process each role
    for (int i = 0; i < PQntuples(res); i++) {
        const char *rolename = PQgetvalue(res, i, i_rolname);
        Oid auth_oid = atooid(PQgetvalue(res, i, i_oid));

        // Skip system roles that slip through the filter
        if (strncmp(rolename, "pg_", 3) == 0) {
            pg_log_warning("role name starting with \"pg_\" skipped (%s)", rolename);
            continue;
        }

        resetPQExpBuffer(buf);

        // Handle binary upgrade OID preservation
        if (binary_upgrade) {
            appendPQExpBuffer(buf,
                "SELECT pg_catalog.binary_upgrade_set_next_pg_authid_oid('%u'::pg_catalog.oid);\n\n",
                auth_oid);
        }

        // Generate CREATE ROLE and ALTER ROLE statements
        if (!binary_upgrade || strcmp(PQgetvalue(res, i, i_is_current_user), "f") == 0) {
            appendPQExpBuffer(buf, "CREATE ROLE %s;\n", fmtId(rolename));
        }
        appendPQExpBuffer(buf, "ALTER ROLE %s WITH", fmtId(rolename));

        // Add all role attributes based on query results
        if (strcmp(PQgetvalue(res, i, i_rolsuper), "t") == 0)
            appendPQExpBufferStr(buf, " SUPERUSER");
        else
            appendPQExpBufferStr(buf, " NOSUPERUSER");

        // ... (similar logic for other boolean attributes)

        // Handle connection limit, password, validity period
        if (strcmp(PQgetvalue(res, i, i_rolconnlimit), "-1") != 0) {
            appendPQExpBuffer(buf, " CONNECTION LIMIT %s",
                            PQgetvalue(res, i, i_rolconnlimit));
        }

        if (!PQgetisnull(res, i, i_rolpassword) && !no_role_passwords) {
            appendPQExpBufferStr(buf, " PASSWORD ");
            appendStringLiteralConn(buf, PQgetvalue(res, i, i_rolpassword), conn);
        }

        appendPQExpBufferStr(buf, ";\n");

        // Add comments and security labels
        if (!no_comments && !PQgetisnull(res, i, i_rolcomment)) {
            appendPQExpBuffer(buf, "COMMENT ON ROLE %s IS ", fmtId(rolename));
            appendStringLiteralConn(buf, PQgetvalue(res, i, i_rolcomment), conn);
            appendPQExpBufferStr(buf, ";\n");
        }

        if (!no_security_labels) {
            buildShSecLabels(conn, "pg_authid", auth_oid, "ROLE", rolename, buf);
        }

        fprintf(OPF, "%s", buf->data);
    }

    // Dump user configurations separately to handle cross-references
    if (PQntuples(res) > 0) {
        fprintf(OPF, "\n--\n-- User Configurations\n--\n");
        for (int i = 0; i < PQntuples(res); i++) {
            dumpUserConfig(conn, PQgetvalue(res, i, i_rolname));
        }
    }

    // Cleanup
    PQclear(res);
    destroyPQExpBuffer(buf);
    fprintf(OPF, "\n\n");
}
```