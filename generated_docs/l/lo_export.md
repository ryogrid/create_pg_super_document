# lo_export

## Location
[src/interfaces/libpq/fe-lobj.c:748-842](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-lobj.c#L748-L842)

## Overview
Client-side function that exports a PostgreSQL large object to a file on the client filesystem, providing complete error handling and cleanup during the transfer process.

## Definition
```c
int lo_export(PGconn *conn, Oid lobjId, const char *filename)
```

## Detailed Description
This function exports a PostgreSQL large object to a file on the client filesystem. It opens the specified large object in read mode, creates or truncates the destination file, and transfers data in chunks. The function implements comprehensive error handling throughout the process, ensuring proper cleanup of both the large object and file descriptors in case of failures.

The function uses buffered reading (LO_BUFSIZE chunks) for efficient data transfer and maintains transaction integrity by carefully managing error states. It follows PostgreSQL's libpq error reporting conventions and preserves meaningful error messages throughout the operation chain.

## Parameters / Member Variables
- `conn`: PostgreSQL database connection handle
- `lobjId`: Object ID of the large object to be exported
- `filename`: Path to the destination file on the client filesystem

## Dependencies
- Functions called/Symbols referenced:
  - [lo_open](lo_open.md)
  - [lo_read](lo_read.md)
  - [lo_close](lo_close.md)
  - open (system call)
  - write (system call)
  - close (system call)
  - pqClearConnErrorState
  - [libpq_append_conn_error](libpq_append_conn_error.md)
  - strerror_r
- Called from (representative examples):
  - [do_lo_export](../d/do_lo_export.md) (psql)
  - [main](../m/main.md) (testlo examples)

## Notes and Other Information
- Returns 1 on success, -1 on failure
- Uses LO_BUFSIZE (8192 bytes) for chunked data transfer
- Creates files with mode 0666 (subject to umask)
- Implements careful error handling to avoid overwriting meaningful error messages
- Automatically clears connection error state when appropriate
- Handles partial write scenarios and file system errors
- Part of the client-side libpq large object interface
- Complementary function to lo_import for large object file operations

## Simplified Source

```c
int lo_export(PGconn *conn, Oid lobjId, const char *filename) {
    int result = 1;
    int fd;
    int nbytes, tmp;
    char buf[LO_BUFSIZE];
    int lobj;
    char sebuf[PG_STRERROR_R_BUFLEN];

    // Open the large object for reading
    lobj = lo_open(conn, lobjId, INV_READ);
    if (lobj == -1)
        return -1;

    // Create/truncate the destination file
    fd = open(filename, O_CREAT | O_WRONLY | O_TRUNC | PG_BINARY, 0666);
    if (fd < 0) {
        int save_errno = errno;
        (void) lo_close(conn, lobj);
        pqClearConnErrorState(conn);
        libpq_append_conn_error(conn, "could not open file \"%s\": %s",
                                filename, strerror_r(save_errno, sebuf, sizeof(sebuf)));
        return -1;
    }

    // Copy data from large object to file in chunks
    while ((nbytes = lo_read(conn, lobj, buf, LO_BUFSIZE)) > 0) {
        tmp = write(fd, buf, nbytes);
        if (tmp != nbytes) {
            int save_errno = errno;
            (void) lo_close(conn, lobj);
            (void) close(fd);
            pqClearConnErrorState(conn);
            libpq_append_conn_error(conn, "could not write to file \"%s\": %s",
                                    filename, strerror_r(save_errno, sebuf, sizeof(sebuf)));
            return -1;
        }
    }

    // Handle read errors and close large object
    if (nbytes < 0 || lo_close(conn, lobj) != 0)
        result = -1;

    // Close file and check for final errors
    if (close(fd) != 0 && result >= 0) {
        libpq_append_conn_error(conn, "could not write to file \"%s\": %s",
                                filename, strerror_r(errno, sebuf, sizeof(sebuf)));
        result = -1;
    }

    return result;
}
```