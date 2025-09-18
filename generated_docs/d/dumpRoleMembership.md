# dumpRoleMembership

## Location
src/bin/pg_dump/pg_dumpall.c: 995 - 1244

## Overview
The dumpRoleMembership function generates SQL GRANT statements for role memberships, ensuring proper ordering to maintain grantor-member relationships and handle version-specific features like grant options.

## Definition


## Detailed Description
The dumpRoleMembership function is a sophisticated component of PostgreSQL's pg_dumpall utility that handles the complex task of dumping role membership relationships while maintaining proper dependency ordering. It must ensure that GRANT statements are emitted in an order where grantors have the necessary ADMIN OPTION privileges before they can grant roles to other users.

The function employs a multi-pass algorithm that processes role memberships role by role. For each role, it starts by allowing only the bootstrap superuser as a valid grantor, then progressively adds users who receive ADMIN OPTION as valid grantors in subsequent passes. This approach handles complex scenarios where role memberships form dependency chains.

The function is version-aware, handling differences between PostgreSQL versions: PostgreSQL 16+ supports explicit grantors and grant-level options (INHERIT, SET), while earlier versions use simplified logic. It also handles orphaned entries gracefully by detecting and warning about OIDs that no longer exist in the system catalogs.

## Parameters / Member Variables
- : PostgreSQL database connection handle used to query system catalogs and determine server version

## Dependencies
- Functions called/Symbols referenced:
  - PQserverVersion (determine PostgreSQL server version for compatibility)
  - printfPQExpBuffer, appendPQExpBuffer, appendPQExpBufferStr (build SQL queries)
  - executeQuery (execute SQL queries against the database)
  - PQgetisnull, PQgetvalue (handle result set data and NULL values)
  - pg_log_warning, pg_log_error (log diagnostic messages)
  - pg_malloc0, pg_free (memory management for tracking arrays)
  - PQfinish, exit_nicely (error handling and cleanup)
  - atooid (convert string OID to numeric OID type)
  - resetPQExpBuffer, destroyPQExpBuffer (manage query buffers)
  - fmtId (format SQL identifiers with proper quoting)
  - rolename_create, rolename_lookup, rolename_insert, rolename_destroy (hash table for grantor tracking)
- Called from:
  - main (in src/bin/pg_dump/pg_dumpall.c after role definitions are dumped)

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