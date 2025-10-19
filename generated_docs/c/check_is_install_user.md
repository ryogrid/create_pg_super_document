# check_is_install_user

## Location
[src/bin/pg_upgrade/check.c:1037-1094](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L1037-L1094)

## Overview
This function validates that the current database user is the PostgreSQL installation user and enforces user restrictions during cluster upgrade operations.

## Definition
```c
static void check_is_install_user(ClusterInfo *cluster)
```

## Detailed Description
The `check_is_install_user` function performs critical user validation during PostgreSQL cluster upgrades. It ensures that the user running pg_upgrade is the same user who originally installed PostgreSQL (the "install user") and enforces that the new cluster contains only this install user.

The function performs two main validations:
1. **Install User Verification**: Queries the current user's role information to verify they are a superuser with the bootstrap superuser OID (typically OID 10). This confirms the user is the original install user.
2. **New Cluster User Restriction**: For new clusters, verifies that only the install user exists. This prevents conflicts during pg_dump restore operations that could occur if user names in the old and new clusters overlap.

The function connects to the template1 database to perform these checks, using pg_roles instead of pg_authid since the latter requires superuser privileges to view.

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing cluster connection and configuration information

## Dependencies
- Functions called/Symbols referenced:
  - [connectToServer](connectToServer.md) (establishes database connection)
  - [prep_status](../p/prep_status.md) (status reporting for user feedback)
  - [executeQueryOrDie](../e/executeQueryOrDie.md) (SQL query execution with error handling)
  - atooid (string to OID conversion)
  - [PQntuples](../P/PQntuples.md) (result tuple count)
  - [PQgetvalue](../P/PQgetvalue.md) (result value extraction)
  - [PQclear](../P/PQclear.md) (result cleanup)
  - [PQfinish](../P/PQfinish.md) (connection cleanup)
  - [check_ok](check_ok.md) (completion status reporting)
  - [pg_fatal](../p/pg_fatal.md) (error reporting and termination)
- Called from (representative examples):
  - [check_and_dump_old_cluster](check_and_dump_old_cluster.md) (old cluster validation)
  - [check_new_cluster](check_new_cluster.md) (new cluster validation)

## Notes and Other Information
- This is a static function, only accessible within the check.c compilation unit
- Uses BOOTSTRAP_SUPERUSERID constant to identify the install user (typically OID 10)
- Cannot use pg_authid for user checks since it requires superuser privileges
- The restriction on new cluster users prevents pg_dump restore conflicts
- Function assumes global os_info variable contains current user information
- Terminates upgrade process with pg_fatal if validation fails
- Connects specifically to template1 database for user queries

## Simplified Source

```c
static void check_is_install_user(ClusterInfo *cluster)
{
    PGresult *res;
    PGconn *conn = connectToServer(cluster, "template1");

    prep_status("Checking database user is the install user");

    // Verify current user is the install user (bootstrap superuser)
    res = executeQueryOrDie(conn,
                           "SELECT rolsuper, oid "
                           "FROM pg_catalog.pg_roles "
                           "WHERE rolname = current_user "
                           "AND rolname !~ '^pg_'");

    // Must be exactly one result with bootstrap superuser OID
    if (PQntuples(res) != 1 ||
        atooid(PQgetvalue(res, 0, 1)) != BOOTSTRAP_SUPERUSERID) {
        pg_fatal("database user \"%s\" is not the install user", os_info.user);
    }
    PQclear(res);

    // Count non-system users in the cluster
    res = executeQueryOrDie(conn,
                           "SELECT COUNT(*) "
                           "FROM pg_catalog.pg_roles "
                           "WHERE rolname !~ '^pg_'");

    if (PQntuples(res) != 1) {
        pg_fatal("could not determine the number of users");
    }

    // New cluster must have only the install user to prevent conflicts
    if (cluster == &new_cluster && strcmp(PQgetvalue(res, 0, 0), "1") != 0) {
        pg_fatal("Only the install user can be defined in the new cluster.");
    }

    PQclear(res);
    PQfinish(conn);
    check_ok();
}
```