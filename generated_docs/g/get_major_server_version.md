# get_major_server_version

## Location
[src/bin/pg_upgrade/server.c:159-190](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/server.c#L159-L190)

## Overview
Retrieves the major PostgreSQL server version number by reading and parsing the PG_VERSION file from the cluster's data directory.

## Definition
```c
uint32 get_major_server_version(ClusterInfo *cluster)
```

## Detailed Description
This function determines the major PostgreSQL version by reading the PG_VERSION file located in the cluster's data directory. It handles both old-style version numbering (e.g., 9.6.1) and new-style version numbering (e.g., 10.1, 11.0) that was introduced in PostgreSQL 10. The function parses the version string and converts it to a standardized integer format for easier version comparisons. For versions prior to 10, it uses the format major*10000 + minor*100, while for version 10 and later, it uses major*10000.

## Parameters / Member Variables
- `cluster`: Pointer to ClusterInfo structure containing the pgdata directory path

## Dependencies
- Functions called/Symbols referenced:
  - snprintf
  - fopen
  - [pg_fatal](../p/pg_fatal.md)
  - fscanf
  - sscanf
  - fclose
- Called from (representative examples):
  - [check_data_dir](../c/check_data_dir.md)
  - fopen_priv

## Notes and Other Information
- Reads PG_VERSION file directly from the PostgreSQL data directory
- Handles version format changes introduced in PostgreSQL 10.x
- Stores the version string in cluster->major_version_str for later reference
- Returns standardized integer representation enabling easy version comparisons
- Uses pg_fatal for error handling - any failure results in program termination
- Essential for pg_upgrade to determine compatibility and required upgrade procedures
- Version format: Pre-10 uses X.Y format, 10+ uses single major number format

## Simplified Source

```c
uint32 get_major_server_version(ClusterInfo *cluster) {
    FILE *version_fd;
    char ver_filename[MAXPGPATH];
    int v1 = 0, v2 = 0;

    // Construct path to PG_VERSION file
    snprintf(ver_filename, sizeof(ver_filename), "%s/PG_VERSION", cluster->pgdata);

    // Open and read version file
    if ((version_fd = fopen(ver_filename, "r")) == NULL)
        pg_fatal("could not open version file \"%s\": %m", ver_filename);

    // Parse version string (e.g., "9.6" or "11")
    if (fscanf(version_fd, "%63s", cluster->major_version_str) == 0 ||
        sscanf(cluster->major_version_str, "%d.%d", &v1, &v2) < 1)
        pg_fatal("could not parse version file \"%s\"", ver_filename);

    fclose(version_fd);

    // Convert to standardized integer format
    if (v1 < 10) {
        // Old style versioning (e.g., 9.6.1 -> 90600)
        return v1 * 10000 + v2 * 100;
    } else {
        // New style versioning (e.g., 10.1 -> 100000)
        return v1 * 10000;
    }
}
```