# dumpRoleMembership

## Location
[src/bin/pg_dump/pg_dumpall.c:995-1244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dumpall.c#L995-L1244)

## Overview
The dumpRoleMembership function generates SQL GRANT statements for role memberships, ensuring proper ordering to maintain grantor-member relationships and handle version-specific features like grant options.

## Definition

```c
static void
dumpRoleMembership(PGconn *conn)
```
## Detailed Description
The dumpRoleMembership function is a sophisticated component of PostgreSQL's pg_dumpall utility that handles the complex task of dumping role membership relationships while maintaining proper dependency ordering. It must ensure that GRANT statements are emitted in an order where grantors have the necessary ADMIN OPTION privileges before they can grant roles to other users.

The function employs a multi-pass algorithm that processes role memberships role by role. For each role, it starts by allowing only the bootstrap superuser as a valid grantor, then progressively adds users who receive ADMIN OPTION as valid grantors in subsequent passes. This approach handles complex scenarios where role memberships form dependency chains.

The function is version-aware, handling differences between PostgreSQL versions: PostgreSQL 16+ supports explicit grantors and grant-level options (INHERIT, SET), while earlier versions use simplified logic. It also handles orphaned entries gracefully by detecting and warning about OIDs that no longer exist in the system catalogs.

## Parameters / Member Variables
- `*conn`: PostgreSQL database connection handle used to query system catalogs and determine server version
## Dependencies
- Functions called/Symbols referenced:
  - [PQserverVersion](../P/PQserverVersion.md) (determine PostgreSQL server version for compatibility)
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md), appendPQExpBuffer, appendPQExpBufferStr (build SQL queries)
  - [executeQuery](../e/executeQuery.md) (execute SQL queries against the database)
  - [PQgetisnull](../P/PQgetisnull.md), PQgetvalue (handle result set data and NULL values)
  - pg_log_warning, pg_log_error (log diagnostic messages)
  - [pg_malloc0](../p/pg_malloc0.md), pg_free (memory management for tracking arrays)
  - [PQfinish](../P/PQfinish.md), exit_nicely (error handling and cleanup)
  - atooid (convert string OID to numeric OID type)
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md), destroyPQExpBuffer (manage query buffers)
  - [fmtId](../f/fmtId.md) (format SQL identifiers with proper quoting)
  - rolename_create, rolename_lookup, rolename_insert, rolename_destroy (hash table for grantor tracking)
- Called from:
  - [main](../m/main.md) (in src/bin/pg_dump/pg_dumpall.c after role definitions are dumped)

## Notes and Other Information
- Function is marked as , indicating it's only used within pg_dumpall.c
- Uses global variables: , , 
- Must be called after dumpRoles() since it assumes roles already exist
- Implements complex dependency resolution using hash tables and multi-pass algorithms
- Version compatibility: PostgreSQL 16+ supports grantors and grant options, earlier versions use simplified logic
- Excludes system-to-system role memberships with 
- Handles orphaned entries (missing role OIDs) gracefully with warning messages
- Uses bootstrap superuser (OID 10) as the always-valid grantor
- Generates GRANT statements with optional clauses: WITH ADMIN OPTION, WITH INHERIT TRUE/FALSE, WITH SET FALSE, GRANTED BY
- Employs sophisticated ordering algorithm to prevent dependency violations in restore scripts
- Orders results by role, member, and grantor (ORDER BY 1,2,3) for consistent processing
- Includes comprehensive error handling for circular dependencies or unresolvable grant chains

## Simplified Source

```c
static void dumpRoleMembership(PGconn *conn) {
    PQExpBuffer buf = createPQExpBuffer();
    PQExpBuffer optbuf = createPQExpBuffer();
    PGresult *res;
    int start = 0, end, total;
    bool dump_grantors = (PQserverVersion(conn) >= 160000);
    bool dump_grant_options = (server_version >= 160000);

    // Build query for role memberships
    printfPQExpBuffer(buf,
        "SELECT ur.rolname AS role, um.rolname AS member, "
        "ug.rolname AS grantor, a.roleid, a.member AS memberid, "
        "a.grantor AS grantorid, a.admin_option");
    if (dump_grant_options) {
        appendPQExpBufferStr(buf, ", a.inherit_option, a.set_option");
    }
    appendPQExpBuffer(buf,
        " FROM pg_auth_members a "
        "LEFT JOIN %s ur ON ur.oid = a.roleid "
        "LEFT JOIN %s um ON um.oid = a.member "
        "LEFT JOIN %s ug ON ug.oid = a.grantor "
        "WHERE NOT (ur.rolname ~ '^pg_' AND um.rolname ~ '^pg_') "
        "ORDER BY 1,2,3", role_catalog, role_catalog, role_catalog);

    res = executeQuery(conn, buf->data);
    total = PQntuples(res);

    if (total > 0) {
        fprintf(OPF, "--\n-- Role memberships\n--\n\n");
    }

    // Process memberships role by role to handle dependency ordering
    while (start < total) {
        char *role = PQgetvalue(res, start, PQfnumber(res, "role"));

        // Handle orphaned entries
        if (PQgetisnull(res, start, PQfnumber(res, "role"))) {
            pg_log_warning("found orphaned pg_auth_members entry");
            break;
        }

        // Find end of current role's memberships
        for (end = start; end < total; ++end) {
            char *otherrole = PQgetvalue(res, end, PQfnumber(res, "role"));
            if (strcmp(role, otherrole) != 0) break;
        }

        // Multi-pass algorithm to handle grantor dependencies
        int remaining = end - start;
        bool *done = pg_malloc0(remaining * sizeof(bool));
        rolename_hash *ht = rolename_create(remaining, NULL);

        while (remaining > 0) {
            int prev_remaining = remaining;

            // Process grants for this role
            for (int i = start; i < end; ++i) {
                if (done[i - start]) continue;

                // Handle orphaned members/grantors
                if (PQgetisnull(res, i, PQfnumber(res, "member")) ||
                    PQgetisnull(res, i, PQfnumber(res, "grantor"))) {
                    pg_log_warning("found orphaned pg_auth_members entry");
                    done[i - start] = true;
                    --remaining;
                    continue;
                }

                char *member = PQgetvalue(res, i, PQfnumber(res, "member"));
                char *grantor = PQgetvalue(res, i, PQfnumber(res, "grantor"));
                char *admin_option = PQgetvalue(res, i, PQfnumber(res, "admin_option"));

                // Check if grantor is allowed (bootstrap superuser or has admin option)
                if (dump_grantors &&
                    atooid(PQgetvalue(res, i, PQfnumber(res, "grantorid"))) != BOOTSTRAP_SUPERUSERID &&
                    rolename_lookup(ht, grantor) == NULL) {
                    continue;
                }

                done[i - start] = true;
                --remaining;

                // Track new grantors with admin option
                if (*admin_option == 't') {
                    bool found;
                    rolename_insert(ht, member, &found);
                }

                // Generate GRANT statement
                resetPQExpBuffer(optbuf);
                fprintf(OPF, "GRANT %s TO %s", fmtId(role), fmtId(member));

                // Add grant options
                if (*admin_option == 't') {
                    appendPQExpBufferStr(optbuf, "ADMIN OPTION");
                }
                if (dump_grant_options) {
                    // Add INHERIT and SET options
                    char *inherit_option = PQgetvalue(res, i, PQfnumber(res, "inherit_option"));
                    char *set_option = PQgetvalue(res, i, PQfnumber(res, "set_option"));
                    // ... option formatting logic
                }

                if (optbuf->data[0] != '\0') {
                    fprintf(OPF, " WITH %s", optbuf->data);
                }
                if (dump_grantors) {
                    fprintf(OPF, " GRANTED BY %s", fmtId(grantor));
                }
                fprintf(OPF, ";\n");
            }

            // Detect infinite loops
            if (remaining == prev_remaining) {
                pg_log_error("could not find a legal dump ordering for memberships in role \"%s\"", role);
                exit_nicely(1);
            }
        }

        rolename_destroy(ht);
        pg_free(done);
        start = end;
    }

    PQclear(res);
    destroyPQExpBuffer(buf);
    fprintf(OPF, "\n\n");
}
```