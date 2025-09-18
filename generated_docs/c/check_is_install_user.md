# check_is_install_user

## Location
src/bin/pg_upgrade/check.c: 1037 - 1094

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
  - connectToServer (establishes database connection)
  - prep_status (status reporting for user feedback)
  - executeQueryOrDie (SQL query execution with error handling)
  - atooid (string to OID conversion)
  - PQntuples (result tuple count)
  - PQgetvalue (result value extraction)
  - PQclear (result cleanup)
  - PQfinish (connection cleanup)
  - check_ok (completion status reporting)
  - pg_fatal (error reporting and termination)
- Called from (representative examples):
  - check_and_dump_old_cluster (old cluster validation)
  - check_new_cluster (new cluster validation)

## Notes and Other Information
- This is a static function, only accessible within the check.c compilation unit
- Uses BOOTSTRAP_SUPERUSERID constant to identify the install user (typically OID 10)
- Cannot use pg_authid for user checks since it requires superuser privileges
- The restriction on new cluster users prevents pg_dump restore conflicts
- Function assumes global os_info variable contains current user information
- Terminates upgrade process with pg_fatal if validation fails
- Connects specifically to template1 database for user queries