# get_bin_version

## Location
[src/bin/pg_upgrade/exec.c:33-84](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/exec.c#L33-L84)

## Overview
This function fetches the major version number of PostgreSQL binaries for a given cluster by executing pg_ctl and parsing its version output.

## Definition

```c
static void
get_bin_version(ClusterInfo *cluster)
```
## Detailed Description
The  function determines the PostgreSQL version of the binaries in a cluster's bin directory. It executes the  command and parses the output to extract version numbers. The function handles both old-style versioning (e.g., 9.6.1) where major version is v1 and minor is v2, and new-style versioning (e.g., 10.1) where v1 is the complete major version number. The parsed version is stored as an integer in the cluster's bin_version field using the formula: major * 10000 + minor * 100 for old style, or major * 10000 for new style.

## Parameters / Member Variables
- `*cluster`: Pointer to ClusterInfo structure that will have its bin_version field populated with the detected version number
## Dependencies
- Functions called/Symbols referenced:
  - snprintf
  - fflush
  - popen
  - fgets
  - [pclose](../p/pclose.md)
  - [pg_fatal](../p/pg_fatal.md)
  - [wait_result_to_str](../w/wait_result_to_str.md)
  - sscanf
- Called from (representative examples):
  - [check_bin_dir](../c/check_bin_dir.md)

## Notes and Other Information
- This is a static function, only accessible within the exec.c file
- The function uses popen to execute shell commands, making it platform-dependent
- Version parsing logic handles the PostgreSQL version numbering change that occurred with version 10
- Error handling includes checks for command execution failure and version parsing failure
- The function modifies the bin_version field of the provided ClusterInfo structure

## Simplified Source

```c
static void get_bin_version(ClusterInfo *cluster) {
    char cmd[MAXPGPATH], cmd_output[MAX_STRING];
    FILE *output;
    int rc, v1 = 0, v2 = 0;

    // Execute pg_ctl --version command
    snprintf(cmd, sizeof(cmd), "\"%s/pg_ctl\" --version", cluster->bindir);
    fflush(NULL);

    // Open command and read version output
    if ((output = popen(cmd, "r")) == NULL ||
        fgets(cmd_output, sizeof(cmd_output), output) == NULL)
        pg_fatal("could not get pg_ctl version data using %s: %m", cmd);

    // Check command execution result
    rc = pclose(output);
    if (rc != 0)
        pg_fatal("could not get pg_ctl version data using %s: %s",
                 cmd, wait_result_to_str(rc));

    // Parse version numbers from output
    if (sscanf(cmd_output, "%*s %*s %d.%d", &v1, &v2) < 1)
        pg_fatal("could not get pg_ctl version output from %s", cmd);

    // Convert to internal version format
    if (v1 < 10) {
        // Old style versioning (e.g., 9.6.1): major.minor.patch
        cluster->bin_version = v1 * 10000 + v2 * 100;
    } else {
        // New style versioning (e.g., 10.1): major.minor
        cluster->bin_version = v1 * 10000;
    }
}
```