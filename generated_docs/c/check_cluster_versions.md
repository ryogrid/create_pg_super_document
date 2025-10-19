# check_cluster_versions

## Location
[src/bin/pg_upgrade/check.c:796-838](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L796-L838)

## Overview
Validates that the old and new PostgreSQL cluster versions are compatible for upgrade and enforces version upgrade constraints.

## Definition

```c
void
check_cluster_versions(void)
```
## Detailed Description
This function performs comprehensive version compatibility checking between the old and new PostgreSQL clusters before proceeding with an upgrade. It enforces several critical constraints:

1. **Minimum Version Support**: Ensures the old cluster is at least PostgreSQL 9.2 or later, as earlier versions are not supported by pg_upgrade.

2. **Target Version Validation**: Verifies that the new cluster matches the current PostgreSQL version that pg_upgrade was compiled for, ensuring compatibility with the upgrade utility itself.

3. **Downgrade Prevention**: Prevents downgrades from newer to older major versions, as this is not supported (pg_dump cannot operate on database versions newer than itself).

4. **Binary/Data Directory Consistency**: Ensures that the PostgreSQL binaries and data directories for both clusters are from matching major versions, preventing configuration mismatches.

The function uses assertions to verify that cluster version information has already been gathered, indicating this check occurs after cluster discovery.

## Parameters / Member Variables

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

## Simplified Source

```c
void check_cluster_versions(void)
{
    prep_status("Checking cluster versions");

    // Ensure cluster versions have been obtained
    Assert(old_cluster.major_version != 0);
    Assert(new_cluster.major_version != 0);

    // Check minimum supported version (PostgreSQL 9.2+)
    if (GET_MAJOR_VERSION(old_cluster.major_version) < 902) {
        pg_fatal("This utility can only upgrade from PostgreSQL version %s and later.", "9.2");
    }

    // Ensure target is current PostgreSQL version
    if (GET_MAJOR_VERSION(new_cluster.major_version) != GET_MAJOR_VERSION(PG_VERSION_NUM)) {
        pg_fatal("This utility can only upgrade to PostgreSQL version %s.", PG_MAJORVERSION);
    }

    // Prevent downgrades (pg_dump can't handle newer versions)
    if (old_cluster.major_version > new_cluster.major_version) {
        pg_fatal("This utility cannot be used to downgrade to older major PostgreSQL versions.");
    }

    // Ensure binaries match data directories for both clusters
    if (GET_MAJOR_VERSION(old_cluster.major_version) != GET_MAJOR_VERSION(old_cluster.bin_version)) {
        pg_fatal("Old cluster data and binary directories are from different major versions.");
    }
    if (GET_MAJOR_VERSION(new_cluster.major_version) != GET_MAJOR_VERSION(new_cluster.bin_version)) {
        pg_fatal("New cluster data and binary directories are from different major versions.");
    }

    check_ok();
}
```