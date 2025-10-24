# dump_lo_buf

## Location
[src/bin/pg_dump/pg_backup_archiver.c:1784-1826](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_archiver.c#L1784-L1826)

## Overview
A function that flushes the current contents of the Large Object (LO) data buffer during LO restoration, handling both direct database connections and file-based output formats.

## Definition

```c
static void
dump_lo_buf(ArchiveHandle *AH)
```
## Detailed Description
The  function manages the output of accumulated Large Object data stored in the archive handle's buffer. When connected directly to a database, it uses the libpq  function to write data directly to the large object. For file-based output, it formats the binary data as a SQL statement using  and outputs it via . After successfully writing the buffer contents, it resets the buffer usage counter to zero.

## Parameters / Member Variables
- `*AH`: Archive handle containing the LO buffer, connection state, and output context
## Dependencies
- Functions called/Symbols referenced:
  - [lo_write](../l/lo_write.md)
  - ngettext
  - pg_log_debug
  - [warn_or_exit_horribly](../w/warn_or_exit_horribly.md)
  - appendByteaLiteralAHX
  - [ahprintf](../a/ahprintf.md)
- Called from (representative examples):
  - TEXT_DUMPALL_HEADER
  - [EndRestoreLO](../E/EndRestoreLO.md)
  - [ahwrite](../a/ahwrite.md)

## Notes and Other Information
- Handles two distinct output modes: direct database write via lo_write and SQL script generation via ahprintf
- Includes debug logging with proper pluralization using ngettext
- Uses a temporary hack by setting writingLO to false to prevent recursive calls to ahwrite
- Assumes no partial writes when using lo_write - any mismatch in written bytes is treated as an error
- Always resets the buffer usage counter to zero after processing

## Simplified Source

```c
static void dump_lo_buf(ArchiveHandle *AH)
{
    if (AH->connection) {
        // Direct database connection: write using libpq
        int res = lo_write(AH->connection, AH->loFd, AH->lo_buf, AH->lo_buf_used);

        pg_log_debug("wrote %zu bytes of large object data (result = %d)",
                     AH->lo_buf_used, res);

        // Check for write errors
        if (res != AH->lo_buf_used)
            warn_or_exit_horribly(AH, "could not write to large object: %s",
                                  PQerrorMessage(AH->connection));
    } else {
        // File-based output: generate SQL statement
        PQExpBuffer buf = createPQExpBuffer();

        appendByteaLiteralAHX(buf, (const unsigned char *) AH->lo_buf,
                              AH->lo_buf_used, AH);

        // Temporarily disable writingLO to prevent recursion
        AH->writingLO = false;
        ahprintf(AH, "SELECT pg_catalog.lowrite(0, %s);\n", buf->data);
        AH->writingLO = true;

        destroyPQExpBuffer(buf);
    }

    // Reset buffer for next use
    AH->lo_buf_used = 0;
}
```