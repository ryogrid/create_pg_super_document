# pickout

## Location
[src/test/examples/testlo64.c:78-112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/examples/testlo64.c#L78-L112)

## Overview
A static function that reads and displays a specified portion of data from a PostgreSQL large object starting at a given offset.

## Definition

```c
static void
pickout(PGconn *conn, Oid lobjId, pg_int64 start, int len)
```
## Detailed Description
The  function provides functionality to extract and display a specific segment of data from a PostgreSQL large object. It opens the large object in read-only mode, seeks to the specified starting position, and reads the requested number of bytes. The function displays the read data to stderr and handles partial reads by continuing to read until the requested length is satisfied or no more data is available.

## Parameters / Member Variables
- `*conn`: Database connection handle for PostgreSQL operations
- `lobjId`: OID of the large object to read from
- `start`: Starting byte position within the large object
- `len`: Number of bytes to read from the starting position
## Dependencies
- Functions called/Symbols referenced:
  - [lo_open](../l/lo_open.md) (PostgreSQL large object opening)
  - [lo_lseek](../l/lo_lseek.md) (PostgreSQL large object seeking)
  - [lo_read](../l/lo_read.md) (PostgreSQL large object reading)
  - [lo_close](../l/lo_close.md) (PostgreSQL large object closing)
  - malloc (memory allocation)
  - free (memory deallocation)
  - fprintf (formatted output to stderr)
  - INV_READ (large object access mode constant)
  - SEEK_SET (seek position constant)
- Called from (representative examples):
  - [main](../m/main.md) (in src/test/examples/testlo.c:255)
  - [main](../m/main.md) (in src/test/examples/testlo64.c:279)

## Notes and Other Information
- This is a static function used in PostgreSQL test examples for demonstrating large object operations
- Uses malloc to allocate a buffer of the requested length plus one byte for null termination
- Handles partial reads by continuing to read until the full requested length is obtained
- Outputs read data directly to stderr for debugging/demonstration purposes
- Properly manages memory by freeing the allocated buffer after use
- Error handling includes basic fprintf statements for debugging
- The function will break out of the read loop if no more data is available (nbytes <= 0)

## Simplified Source

```c
static void
pickout(PGconn *conn, Oid lobjId, int start, int len)
{
    int lobj_fd;
    char *buf;
    int nbytes, nread;

    // Open large object for reading
    lobj_fd = lo_open(conn, lobjId, INV_READ);
    if (lobj_fd < 0)
        fprintf(stderr, "cannot open large object %u", lobjId);

    // Seek to starting position and allocate buffer
    lo_lseek(conn, lobj_fd, start, SEEK_SET);
    buf = malloc(len + 1);

    // Read data in chunks until complete
    nread = 0;
    while (len - nread > 0) {
        nbytes = lo_read(conn, lobj_fd, buf, len - nread);
        buf[nbytes] = '\0';
        fprintf(stderr, ">>> %s", buf);
        nread += nbytes;
        if (nbytes <= 0)
            break;  // No more data available
    }

    // Cleanup
    free(buf);
    fprintf(stderr, "\n");
    lo_close(conn, lobj_fd);
}
```