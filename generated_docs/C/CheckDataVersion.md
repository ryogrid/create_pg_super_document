# CheckDataVersion

## Location
[src/bin/pg_resetwal/pg_resetwal.c:518-558](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_resetwal/pg_resetwal.c#L518-L558)

## Overview
CheckDataVersion validates that the PostgreSQL data directory version is compatible with the pg_resetwal utility to prevent major version mismatches that could cause corruption.

## Definition

```c
static void
CheckDataVersion(void)
```
## Detailed Description
This function performs a critical safety check by reading and verifying the PostgreSQL major version stored in the PG_VERSION file within the data directory. The primary purpose is to prevent the pg_resetwal utility from operating on a data directory that belongs to a different major PostgreSQL version, which could lead to serious corruption.

The function reads the first line from the PG_VERSION file, strips whitespace, and compares it against the current program's major version (PG_MAJORVERSION). If there's a mismatch, it logs detailed error information and terminates the program with exit code 1.

Unlike pg_control corruption (which pg_resetwal is designed to handle), a corrupted or mismatched PG_VERSION file is considered a serious error that should halt execution, as this file is unlikely to become corrupted accidentally and version mismatches usually indicate user error.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - fopen (file opening)
  - fgets (reading file content)
  - ferror (checking file errors)
  - [pg_strip_crlf](../p/pg_strip_crlf.md) (whitespace removal)
  - strcmp (string comparison)
  - pg_log_error (error logging)
  - pg_log_error_detail (detailed error logging)
  - fclose (file closing)
  - exit (program termination)
- Called from:
  - [main](../m/main.md) (in pg_resetwal.c:361)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the pg_resetwal.c source file
- The function performs early validation to prevent catastrophic errors from version mismatches
- Unlike pg_control validation, PG_VERSION mismatches are treated as fatal errors
- The function expects the version string to be on the first line of the PG_VERSION file
- Error handling includes both file I/O errors and version compatibility checks
- The function uses pg_fatal and pg_log_error for different types of errors, providing appropriate detail levels

## Simplified Source

```c
static void CheckDataVersion(void) {
    const char *ver_file = "PG_VERSION";
    FILE *ver_fd;
    char rawline[64];

    // Open PG_VERSION file
    if ((ver_fd = fopen(ver_file, "r")) == NULL)
        pg_fatal("could not open file \"%s\" for reading: %m", ver_file);

    // Read first line containing version number
    if (!fgets(rawline, sizeof(rawline), ver_fd)) {
        if (!ferror(ver_fd))
            pg_fatal("unexpected empty file \"%s\"", ver_file);
        else
            pg_fatal("could not read file \"%s\": %m", ver_file);
    }

    // Strip trailing whitespace
    pg_strip_crlf(rawline);

    // Compare with current program version
    if (strcmp(rawline, PG_MAJORVERSION) != 0) {
        pg_log_error("data directory is of wrong version");
        pg_log_error_detail("File \"%s\" contains \"%s\", which is not compatible with this program's version \"%s\".",
                           ver_file, rawline, PG_MAJORVERSION);
        exit(1);
    }

    fclose(ver_fd);
}
```