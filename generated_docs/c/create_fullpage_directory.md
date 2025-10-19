# create_fullpage_directory

## Location
[src/bin/pg_waldump/pg_waldump.c:128-160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_waldump/pg_waldump.c#L128-L160)

## Overview
Creates a directory for storing full-page images extracted from WAL records, ensuring the directory is empty and properly accessible.

## Definition
```c
static void create_fullpage_directory(char *path)
```

## Detailed Description
The create_fullpage_directory function is responsible for setting up a directory where pg_waldump can store full-page images extracted from WAL (Write-Ahead Log) records. It performs comprehensive validation and creation logic: first checking if the directory exists using pg_check_dir(), then taking appropriate action based on the result. If the directory doesn't exist, it creates it using pg_mkdir_p() with proper permissions. If it exists and is empty, the function proceeds without error. However, if the directory exists but is not empty, or if there are access issues, the function terminates the program with a fatal error to prevent overwriting existing data.

## Parameters / Member Variables
- `path`: A null-terminated string containing the path where the full-page directory should be created

## Dependencies
- Functions called/Symbols referenced:
  - [pg_check_dir](../p/pg_check_dir.md) (function to check directory status and accessibility)
  - [pg_mkdir_p](../p/pg_mkdir_p.md) (function to create directory with parent directories as needed)
  - pg_dir_create_mode (global variable defining directory creation permissions)
  - [pg_fatal](../p/pg_fatal.md) (function to report fatal errors and exit)
- Called from (representative examples):
  - [main](../m/main.md) (called in pg_waldump.c:1110)

## Notes and Other Information
- The function enforces a strict policy: the target directory must be either non-existent or empty
- Uses pg_mkdir_p() which creates parent directories if necessary, similar to 'mkdir -p'
- Fatal errors are reported with detailed error messages including system error details (%m)
- The directory creation uses pg_dir_create_mode permissions, typically 0700 (owner read/write/execute only)
- Return codes from pg_check_dir(): 0 = doesn't exist, 1 = exists and empty, 2/3/4 = exists and not empty, -1 = access error
- This function is part of pg_waldump's full-page image extraction feature, which helps with debugging and analysis

## Simplified Source

```c
static void create_fullpage_directory(char *path) {
    int ret = pg_check_dir(path);

    switch (ret) {
        case 0:
            // Directory doesn't exist - create it
            if (pg_mkdir_p(path, pg_dir_create_mode) < 0)
                pg_fatal("could not create directory \"%s\": %m", path);
            break;

        case 1:
            // Directory exists and is empty - good to proceed
            break;

        case 2:
        case 3:
        case 4:
            // Directory exists but is not empty - error
            pg_fatal("directory \"%s\" exists but is not empty", path);
            break;

        default:
            // Cannot access directory - error
            pg_fatal("could not access directory \"%s\": %m", path);
    }
}
```