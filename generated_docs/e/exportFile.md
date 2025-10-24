# exportFile

## Location
[src/test/examples/testlo64.c:172-214](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/examples/testlo64.c#L172-L214)

## Overview
A static function that exports a PostgreSQL large object to a file on the Unix filesystem by copying its contents.

## Definition

```c
static void
exportFile(PGconn *conn, Oid lobjId, char *filename)
```
## Detailed Description
The  function provides functionality to export a PostgreSQL large object to an external Unix file. It opens the specified large object in read-only mode, creates or truncates the target file, and copies the large object's contents to the file using a buffered read/write approach. The function handles the complete export process including proper file creation, data transfer in chunks, and cleanup of both database and file descriptors.

## Parameters / Member Variables
- `*conn`: Database connection handle for PostgreSQL operations
- `lobjId`: OID of the large object to export from the database
- `*filename`: Path to the Unix file where the large object contents will be written
## Dependencies
- Functions called/Symbols referenced:
  - [lo_open](../l/lo_open.md) (PostgreSQL large object opening)
  - [lo_read](../l/lo_read.md) (PostgreSQL large object reading)
  - [lo_close](../l/lo_close.md) (PostgreSQL large object closing)
  - open (Unix system call for file operations)
  - write (Unix system call for writing file data)
  - close (Unix system call for closing file)
  - fprintf (formatted output to stderr)
  - BUFSIZE (buffer size constant)
  - INV_READ (large object access mode constant)
  - O_CREAT, O_WRONLY, O_TRUNC (file creation and access mode constants)
- Called from (representative examples):
  - No direct callers found (static function in test example)

## Notes and Other Information
- This is a static function located in the test examples directory, primarily used for demonstration purposes
- Creates the target file with permissions 0666 (read/write for owner, group, and others)
- Uses file creation flags O_CREAT | O_WRONLY | O_TRUNC to create/truncate the output file
- Uses a buffer size defined by BUFSIZE constant for efficient data copying
- Opens large objects with read-only permission (INV_READ) for safety
- Error handling includes basic fprintf statements for debugging file and write operations
- Performs the inverse operation of importFile - extracting data from large objects to external files
- Part of the testlo.c example demonstrating bidirectional large object file operations in PostgreSQL
- Properly handles partial writes by checking if the number of bytes written matches what was read

## Simplified Source

```c
static void exportFile(PGconn *conn, Oid lobjId, char *filename) {
    int lobj_fd, fd;
    char buf[BUFSIZE];
    int nbytes, tmp;

    // Open large object for reading
    lobj_fd = lo_open(conn, lobjId, INV_READ);
    if (lobj_fd < 0) {
        fprintf(stderr, "cannot open large object %u", lobjId);
        return;
    }

    // Create/truncate output file
    fd = open(filename, O_CREAT | O_WRONLY | O_TRUNC, 0666);
    if (fd < 0) {
        fprintf(stderr, "cannot open unix file\"%s\"", filename);
        lo_close(conn, lobj_fd);
        return;
    }

    // Copy large object contents to file in chunks
    while ((nbytes = lo_read(conn, lobj_fd, buf, BUFSIZE)) > 0) {
        tmp = write(fd, buf, nbytes);
        if (tmp < nbytes) {
            fprintf(stderr, "error while writing \"%s\"", filename);
            break;
        }
    }

    // Clean up file descriptors
    lo_close(conn, lobj_fd);
    close(fd);
}
```