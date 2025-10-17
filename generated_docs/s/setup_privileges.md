# setup_privileges

## Location
[src/bin/initdb/initdb.c:1786-1926](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L1786-L1926)

## Overview
Sets up default privileges for system catalogs and populates the pg_init_privs table to preserve privilege information at database initialization time.

## Definition

```c
static void
setup_privileges(FILE *cmdfd)
```
## Detailed Description
The setup_privileges function is responsible for establishing initial access permissions on PostgreSQL system catalogs during database initialization. It performs two main tasks:

1. **System Catalog Privileges**: Marks most system catalogs as world-readable by updating their Access Control Lists (ACLs). The function carefully preserves any existing privilege sets that have already been configured (NOT NULL values).

2. **pg_init_privs Population**: Populates the pg_init_privs system catalog with the initial privilege state of database objects. This information is crucial for pg_dump to preserve user-modified privileges across dump/reload operations and pg_upgrade processes.

The function handles privileges for various object types including relations (tables, views, materialized views, sequences), attributes (columns), procedures, types, languages, large objects, namespaces, foreign data wrappers, and foreign servers. Note that databases and tablespaces are excluded since pg_init_privs only tracks per-database objects.

## Parameters / Member Variables
- `*cmdfd`: FILE pointer to the command file where SQL statements are written for execution during database initialization
## Dependencies
- Functions called/Symbols referenced:
  - PG_CMD_PRINTF (macro for formatted SQL output)
  - PG_CMD_PUTS (macro for SQL string output)
  - [escape_quotes](../e/escape_quotes.md) (function to escape quotes in strings)
  - CppAsString2 (macro for stringifying constants)
  - RELKIND_* constants (relation kind identifiers)
  - BOOTSTRAP_SUPERUSERID constant

- Called from:
  - [initialize_data_directory](../i/initialize_data_directory.md) (main initialization function)

## Notes and Other Information
- This function is critical for PostgreSQL security model initialization
- The privilege setup ensures backward compatibility for pg_dump and pg_upgrade operations
- The function uses SQL commands written to cmdfd rather than direct database API calls
- Special handling is provided for large objects, which have their public access revoked by default
- The 'i' privtype in pg_init_privs indicates initial/installation privileges

## Simplified Source

```c
static void setup_privileges(FILE *cmdfd) {
    // Grant read access to system catalogs for public
    PG_CMD_PRINTF("UPDATE pg_class "
                  "  SET relacl = (SELECT array_agg(a.acl) FROM "
                  " (SELECT E'=r/\"%s\"' as acl "
                  "  UNION SELECT unnest(pg_catalog.acldefault("
                  "    CASE WHEN relkind = 'S' THEN 's' "
                  "         ELSE 'r' END::\"char\",%d::oid))"
                  " ) as a) "
                  "  WHERE relkind IN ('r', 'v', 'm', 'S')"
                  "  AND relacl IS NULL;\n\n",
                  escape_quotes(username), BOOTSTRAP_SUPERUSERID);

    // Grant schema usage to public
    PG_CMD_PUTS("GRANT USAGE ON SCHEMA pg_catalog, public TO PUBLIC;\n\n");

    // Secure large objects
    PG_CMD_PUTS("REVOKE ALL ON pg_largeobject FROM PUBLIC;\n\n");

    // Populate pg_init_privs for various object types
    // This preserves initial privileges for pg_dump/pg_upgrade

    // Relations (tables, views, materialized views, sequences)
    PG_CMD_PUTS("INSERT INTO pg_init_privs "
                "  (objoid, classoid, objsubid, initprivs, privtype)"
                "    SELECT oid, (SELECT oid FROM pg_class WHERE relname = 'pg_class'),"
                "        0, relacl, 'i'"
                "    FROM pg_class"
                "    WHERE relacl IS NOT NULL"
                "        AND relkind IN ('r', 'v', 'm', 'S');\n\n");

    // Attributes (columns), procedures, types, languages, etc.
    // Multiple INSERT statements for different object types...
}
```