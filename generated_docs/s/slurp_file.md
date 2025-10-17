# slurp_file

## Location
[src/bin/pg_combinebackup/pg_combinebackup.c:1376-1409](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/pg_combinebackup.c#L1376-L1409)

## Overview
Reads the entire contents of a file into a StringInfo buffer with size validation and error handling, ensuring safe file I/O operations within specified limits.

## Definition
```c
static void slurp_file(int fd, char *filename, StringInfo buf, int maxlen)
```

## Detailed Description
This function performs a complete file read operation by first checking the file size against a maximum limit, then reading the entire file contents into a StringInfo buffer. It uses fstat to determine file size before reading to ensure the file is not larger than the specified maximum. The function enlarges the StringInfo buffer to accommodate the file data, performs a single read operation, and validates that the exact expected number of bytes was read. It maintains the StringInfo trailing null-byte invariant after reading the data.

## Parameters / Member Variables
- `fd`: File descriptor for the file to read (already opened)
- `filename`: Filename string used for error reporting purposes
- `buf`: StringInfo buffer to store the file contents
- `maxlen`: Maximum allowed file size in bytes - files larger than this cause fatal errors

## Dependencies
- Functions called/Symbols referenced:
  - ssize_t (POSIX type for signed size values)
  - fstat (system call to get file status)
  - [enlargeStringInfo](../e/enlargeStringInfo.md) (PostgreSQL function to expand StringInfo capacity)
  - read (system call for file reading)
- Called from (representative examples):
  - [check_backup_label_files](../c/check_backup_label_files.md) (in src/bin/pg_combinebackup/pg_combinebackup.c:534)
  - [read_pg_version_file](../r/read_pg_version_file.md) (in src/bin/pg_combinebackup/pg_combinebackup.c:1171)

## Notes and Other Information
- This is a static function used specifically within pg_combinebackup utility
- Expects no concurrent file modifications during the read operation
- Performs complete file validation including size checking before reading
- Automatically handles StringInfo buffer management and null-termination
- Fatal errors occur if file is too large or read operation fails/is incomplete
- Designed for reading small configuration or metadata files safely
- File location: src/bin/pg_combinebackup/pg_combinebackup.c:1376-1409

## Simplified Source

```c
static void slurp_file(int fd, char *filename, StringInfo buf, int maxlen) {
    struct stat st;
    ssize_t rb;

    // Check file size
    if (fstat(fd, &st) != 0)
        pg_fatal("could not stat file \"%s\": %m", filename);
    if (st.st_size > maxlen)
        pg_fatal("file \"%s\" is too large", filename);

    // Prepare buffer space
    enlargeStringInfo(buf, st.st_size);

    // Read entire file
    rb = read(fd, &buf->data[buf->len], st.st_size);
    if (rb != st.st_size) {
        if (rb < 0)
            pg_fatal("could not read file \"%s\": %m", filename);
        else
            pg_fatal("could not read file \"%s\": read %zd of %lld",
                     filename, rb, (long long int) st.st_size);
    }

    // Update buffer length and maintain null termination
    buf->len += rb;
    buf->data[buf->len] = '\0';
}
```