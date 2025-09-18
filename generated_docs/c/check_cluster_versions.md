# check_cluster_versions

## Location
[src/bin/pg_upgrade/check.c:796-838](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L796-L838)

## Overview
Validates that the old and new PostgreSQL cluster versions are compatible for upgrade and enforces version upgrade constraints.

## Definition


## Detailed Description
This function performs comprehensive version compatibility checking between the old and new PostgreSQL clusters before proceeding with an upgrade. It enforces several critical constraints:

1. **Minimum Version Support**: Ensures the old cluster is at least PostgreSQL 9.2 or later, as earlier versions are not supported by pg_upgrade.

2. **Target Version Validation**: Verifies that the new cluster matches the current PostgreSQL version that pg_upgrade was compiled for, ensuring compatibility with the upgrade utility itself.

3. **Downgrade Prevention**: Prevents downgrades from newer to older major versions, as this is not supported (pg_dump cannot operate on database versions newer than itself).

4. **Binary/Data Directory Consistency**: Ensures that the PostgreSQL binaries and data directories for both clusters are from matching major versions, preventing configuration mismatches.

The function uses assertions to verify that cluster version information has already been gathered, indicating this check occurs after cluster discovery.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  -  (displays status message)
  -  (macro to extract major version number)
  -  (terminates with error message)
  -  (marks status as successful)
  -  (debug assertion macro)
  -  (old cluster version)
  -  (new cluster version)  
  -  (old cluster binary version)
  -  (new cluster binary version)
  -  (compiled PostgreSQL version number)
  -  (compiled PostgreSQL major version string)
- Called from (representative examples):
  -  (in src/bin/pg_upgrade/pg_upgrade.c:130)

## Notes and Other Information
- This function is called early in the pg_upgrade process to catch version incompatibilities before any modifications are made
- The 9.2 minimum version requirement reflects the earliest version supported by modern pg_upgrade
- Alpha/beta upgrades within the same major version are permitted
- The binary/data directory version matching prevents subtle errors that could occur from version mismatches
- All version checks use the GET_MAJOR_VERSION macro for consistent version comparison logic
- Fatal errors from this function will terminate pg_upgrade immediately with descriptive error messages