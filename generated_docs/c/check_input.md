# check_input

## Location
[src/bin/initdb/initdb.c:988-1018](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L988-L1018)

## Overview
A static utility function in initdb that validates the existence and accessibility of input files required during database cluster initialization.

## Definition

```c
struct stat statbuf;
```
## Detailed Description
The  function performs file system validation on input files that are essential for PostgreSQL database cluster initialization. It first attempts to stat the file at the given path, checking for file existence and accessibility. If the file cannot be accessed, it distinguishes between file-not-found errors (ENOENT) and other access errors, providing appropriate error messages for each case. Additionally, it verifies that the path points to a regular file (not a directory, device, or other special file type) using the  macro. If any validation fails, the function logs descriptive error messages with hints about possible causes and terminates the program with exit code 1.

## Parameters / Member Variables
- : The file path to be validated for existence, accessibility, and regular file type

## Dependencies
- Functions called/Symbols referenced:
  - [stat](../s/stat.md) (system call to get file status)
  - pg_log_error (PostgreSQL error logging function)
  - pg_log_error_hint (PostgreSQL error hint logging function)
  - S_ISREG (macro to check if file is a regular file)
  - exit (system function to terminate the program)
- Called from (representative examples):
  - [setup_data_file_paths](../s/setup_data_file_paths.md) (called multiple times to validate template and configuration files)

## Notes and Other Information
- This is a static function, only accessible within initdb.c
- The function is fatal - it will terminate the program (exit(1)) if any validation fails
- Used to validate critical PostgreSQL template files like pg_hba.conf, pg_ident.conf, postgresql.conf, etc.
- Provides helpful error hints suggesting the user may have a corrupted installation or incorrect -L directory option
- The function uses errno to distinguish between different types of file access failures
- Works in conjunction with  function - typically  constructs the path and  validates it

## Simplified Source

```c
static void
check_input(char *path)
{
    struct stat statbuf;

    // Check if file exists and is accessible
    if (stat(path, &statbuf) != 0) {
        if (errno == ENOENT) {
            pg_log_error("file \"%s\" does not exist", path);
        } else {
            pg_log_error("could not access file \"%s\": %m", path);
        }
        pg_log_error_hint("This might mean you have a corrupted installation or identified the wrong directory with the invocation option -L.");
        exit(1);
    }

    // Verify it's a regular file
    if (!S_ISREG(statbuf.st_mode)) {
        pg_log_error("file \"%s\" is not a regular file", path);
        pg_log_error_hint("This might mean you have a corrupted installation or identified the wrong directory with the invocation option -L.");
        exit(1);
    }
}
```