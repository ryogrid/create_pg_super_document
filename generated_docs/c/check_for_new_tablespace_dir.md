# check_for_new_tablespace_dir

## Location
[src/bin/pg_upgrade/check.c:885-913](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/check.c#L885-L913)

## Overview
This function verifies that new cluster tablespace directories do not already exist, preventing conflicts during PostgreSQL upgrade operations that could occur from previous failed upgrade attempts.

## Definition
```c
static void check_for_new_tablespace_dir(void)
```

## Detailed Description
The `check_for_new_tablespace_dir` function performs a preventive check to detect potential conflicts with tablespace directories from previous pg_upgrade runs. When a previous upgrade attempt fails, users might recreate the new cluster directory but forget to remove the associated tablespace directories. This function identifies such situations early in the upgrade process, before schema restoration begins.

The function operates by:
1. Iterating through all old tablespaces stored in `os_info.old_tablespaces`
2. Constructing the expected new tablespace directory path by appending the new cluster's tablespace suffix
3. Using `stat()` to check if each directory already exists
4. Terminating with a fatal error if any conflicting directories are found

This early detection prevents errors that would otherwise occur during the global object restoration phase, making troubleshooting easier for users.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md) (status reporting for user feedback)
  - [check_ok](check_ok.md) (completion status reporting)
  - snprintf (string formatting)
  - [stat](../s/stat.md) (file system status checking)
  - [pg_fatal](../p/pg_fatal.md) (error reporting and termination)
- Called from (representative examples):
  - [check_new_cluster](check_new_cluster.md) (main cluster validation function)

## Notes and Other Information
- This is a static function, only accessible within the check.c compilation unit
- Uses MAXPGPATH constant for path buffer sizing
- Checks both successful stat() calls and errno != ENOENT to detect existing directories
- Provides user-friendly status messages via prep_status and check_ok
- Critical for preventing silent failures during schema restoration phase
- The function assumes os_info and new_cluster global variables are properly initialized