# read_pg_version_file

## Location
[src/bin/pg_combinebackup/pg_combinebackup.c:1154-1204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/pg_combinebackup.c#L1154-L1204)

## Overview
Reads the PostgreSQL version number from the PG_VERSION file in a specified directory and converts it to the standard server version number format used internally.

## Definition
```c
static int read_pg_version_file(char *directory)
```

## Detailed Description
This function constructs the path to the PG_VERSION file within the given directory, reads its contents, and parses the version number string. The function converts the version string (e.g., "14\n") to PostgreSQL's internal version numbering scheme by multiplying by 10000 (e.g., 140000). It includes error handling for file operations and version parsing, with specific checks to reject very old PostgreSQL versions that used multi-part version numbers (like 9.6 or 8.4) as they are not relevant to incremental backup functionality.

## Parameters / Member Variables
- `directory`: Path to the directory containing the PG_VERSION file to read

## Dependencies
- Functions called/Symbols referenced:
  - open (system call for file opening)
  - [slurp_file](../s/slurp_file.md) (utility function to read file contents into StringInfo)
  - close (system call for file closing)
  - pg_log_debug (logging function for debug output)
- Called from (representative examples):
  - [main](../m/main.md) (in src/bin/pg_combinebackup/pg_combinebackup.c:269)

## Notes and Other Information
- This is a static function used specifically within pg_combinebackup utility
- The function enforces a length limit of 128 bytes when reading the PG_VERSION file
- Returns version number in PostgreSQL's internal format (major version * 10000)
- Includes specific error handling for old PostgreSQL versions with multi-part version numbers
- Uses StringInfo for safe string handling and memory management
- File location: src/bin/pg_combinebackup/pg_combinebackup.c:1154-1204

## Simplified Source

```c
static int read_pg_version_file(char *directory) {
    char filename[MAXPGPATH];
    StringInfoData buf;
    int fd, version;
    char *ep;

    // Open PG_VERSION file
    snprintf(filename, MAXPGPATH, "%s/PG_VERSION", directory);
    if ((fd = open(filename, O_RDONLY, 0)) < 0)
        pg_fatal("could not open file \"%s\": %m", filename);

    // Read file contents with size limit
    initStringInfo(&buf);
    slurp_file(fd, filename, &buf, 128);
    close(fd);

    // Parse version number
    errno = 0;
    version = strtoul(buf.data, &ep, 10);
    if (errno != 0 || *ep != '\n') {
        // Reject old multi-part version numbers (e.g., 9.6)
        if (version < 10 && *ep == '.')
            pg_fatal("%s: server version too old", filename);
        pg_fatal("%s: could not parse version number", filename);
    }

    pfree(buf.data);
    return version * 10000;  // Convert to internal format (e.g., 14 -> 140000)
}
```