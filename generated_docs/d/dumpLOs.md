# dumpLOs

## Location
[src/bin/pg_dump/pg_dump.c:3904-3949](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_dump.c#L3904-L3949)

## Overview
Dumps the data contents of large objects (LOBs) in PostgreSQL, reading them from the database and writing them to the archive output.

## Definition

```c
static int
dumpLOs(Archive *fout, const void *arg)
```
## Detailed Description
The  function handles the dumping of large object data during a pg_dump operation. It takes a  structure containing an array of large object OIDs and iterates through each one, opening the large object, reading its contents in chunks, and writing the data to the archive. The function uses the PostgreSQL large object API (, , ) to access the binary data and outputs it through the archive's  interface.

The function logs progress information and handles errors that may occur during large object access, such as failure to open a large object or read errors during data transfer.

## Parameters / Member Variables
- : Archive pointer for output operations and database connection
- : Void pointer that should be cast to  containing large object information including:
  - : Number of large objects to dump
  - : Array of large object OIDs to process

## Dependencies
- Functions called/Symbols referenced:
  -  (cast from arg parameter)
  -  (get database connection)
  -  (logging)
  - , ,  (PostgreSQL large object API)
  - ,  (archive LO boundary markers)
  -  (write data to archive)
  -  (buffer size constant)
  -  (large object read mode constant)
- Called from (representative examples):
  -  (main dump dispatch function)

## Notes and Other Information
- Returns 1 on successful completion
- Uses a fixed buffer size () for reading large object data in chunks
- Handles large objects that may be larger than available memory by streaming the data
- Part of the pg_dump utility's infrastructure for backing up PostgreSQL databases
- Large objects are a PostgreSQL-specific feature for storing binary data outside of regular table storage

## Simplified Source

```c
static int
dumpLOs(Archive *fout, const void *arg)
{
    const LoInfo *loinfo = (const LoInfo *) arg;
    PGconn *conn = GetConnection(fout);
    char buf[LOBBUFSIZE];

    pg_log_info("saving large objects \"%s\"", loinfo->dobj.name);

    // Process each large object in the group
    for (int i = 0; i < loinfo->numlos; i++) {
        Oid loOid = loinfo->looids[i];

        // Open large object for reading
        int loFd = lo_open(conn, loOid, INV_READ);
        if (loFd == -1)
            pg_fatal("could not open large object %u: %s",
                    loOid, PQerrorMessage(conn));

        // Mark start of LO data in archive
        StartLO(fout, loOid);

        // Read and write LO data in chunks
        int cnt;
        do {
            cnt = lo_read(conn, loFd, buf, LOBBUFSIZE);
            if (cnt < 0)
                pg_fatal("error reading large object %u: %s",
                        loOid, PQerrorMessage(conn));

            WriteData(fout, buf, cnt);
        } while (cnt > 0);

        // Close LO and mark end in archive
        lo_close(conn, loFd);
        EndLO(fout, loOid);
    }

    return 1; // Success
}
```