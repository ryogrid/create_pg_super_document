# ReadTwoPhaseFile

## Location
[src/backend/access/transam/twophase.c:1287-1403](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L1287-L1403)

## Overview
ReadTwoPhaseFile reads and validates a two-phase commit state file from disk, performing integrity checks before returning the file contents.

## Definition

```c
struct stat stat;
```
## Detailed Description
ReadTwoPhaseFile is responsible for securely reading two-phase commit state files from the filesystem and validating their integrity. It constructs the file path using the transaction ID, opens the file with proper error handling, validates file size constraints, reads the entire file contents into memory, and performs comprehensive validation including magic number verification and CRC checksum validation. The function supports a missing_ok parameter to handle recovery scenarios where files may legitimately not exist.

## Parameters / Member Variables
- : TransactionId of the prepared transaction whose state file should be read
- : bool flag indicating whether missing files should return NULL instead of throwing an error (used during recovery)

## Dependencies
- Functions called/Symbols referenced:
  - [TwoPhaseFilePath](../T/TwoPhaseFilePath.md)
  - [OpenTransientFile](../O/OpenTransientFile.md)
  - fstat
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)
  - [pgstat_report_wait_end](../p/pgstat_report_wait_end.md)
  - [CloseTransientFile](../C/CloseTransientFile.md)
  - INIT_CRC32C
  - COMP_CRC32C
  - FIN_CRC32C
  - EQ_CRC32C
- Called from (representative examples):
  - [StandbyTransactionIdIsPrepared](../S/StandbyTransactionIdIsPrepared.md)
  - [FinishPreparedTransaction](../F/FinishPreparedTransaction.md)
  - [ProcessTwoPhaseBuffer](../P/ProcessTwoPhaseBuffer.md)
  - [LookupGXact](../L/LookupGXact.md)

## Notes and Other Information
- Static function (internal to twophase.c module)
- Validates file size is between minimum required size and MaxAllocSize to prevent memory issues
- Performs CRC alignment check to detect corruption
- Uses WAIT_EVENT_TWOPHASE_FILE_READ for wait event reporting during file I/O
- Magic number validation ensures file format correctness (TWOPHASE_MAGIC)
- Total length validation cross-checks header field against actual file size
- CRC32C checksum validation ensures data integrity
- Returns palloc'd buffer that caller must free
- Critical for recovery operations and prepared transaction processing

## Simplified Source

```c
static char *ReadTwoPhaseFile(TransactionId xid, bool missing_ok)
{
    char path[MAXPGPATH];
    char *buf;
    TwoPhaseFileHeader *hdr;
    int fd;
    struct stat stat;
    uint32 crc_offset;
    pg_crc32c calc_crc, file_crc;
    int r;

    // Construct file path
    TwoPhaseFilePath(path, xid);

    // Open file
    fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
    if (fd < 0)
    {
        if (missing_ok && errno == ENOENT)
            return NULL;
        ereport(ERROR, (errcode_for_file_access(),
                       errmsg("could not open file \"%s\": %m", path)));
    }

    // Check file size constraints
    if (fstat(fd, &stat))
        ereport(ERROR, (errcode_for_file_access(),
                       errmsg("could not stat file \"%s\": %m", path)));

    if (stat.st_size < (MAXALIGN(sizeof(TwoPhaseFileHeader)) +
                        MAXALIGN(sizeof(TwoPhaseRecordOnDisk)) +
                        sizeof(pg_crc32c)) ||
        stat.st_size > MaxAllocSize)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                       errmsg_plural("incorrect size of file \"%s\": %lld byte",
                                    "incorrect size of file \"%s\": %lld bytes",
                                    (long long int) stat.st_size, path,
                                    (long long int) stat.st_size)));

    // Verify CRC alignment
    crc_offset = stat.st_size - sizeof(pg_crc32c);
    if (crc_offset != MAXALIGN(crc_offset))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                       errmsg("incorrect alignment of CRC offset for file \"%s\"",
                              path)));

    // Read entire file
    buf = (char *) palloc(stat.st_size);
    pgstat_report_wait_start(WAIT_EVENT_TWOPHASE_FILE_READ);
    r = read(fd, buf, stat.st_size);
    if (r != stat.st_size)
    {
        if (r < 0)
            ereport(ERROR, (errcode_for_file_access(),
                           errmsg("could not read file \"%s\": %m", path)));
        else
            ereport(ERROR, (errmsg("could not read file \"%s\": read %d of %lld",
                                   path, r, (long long int) stat.st_size)));
    }
    pgstat_report_wait_end();

    CloseTransientFile(fd);

    // Validate file format
    hdr = (TwoPhaseFileHeader *) buf;
    if (hdr->magic != TWOPHASE_MAGIC)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                       errmsg("invalid magic number stored in file \"%s\"", path)));

    if (hdr->total_len != stat.st_size)
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                       errmsg("invalid size stored in file \"%s\"", path)));

    // Verify CRC checksum
    INIT_CRC32C(calc_crc);
    COMP_CRC32C(calc_crc, buf, crc_offset);
    FIN_CRC32C(calc_crc);

    file_crc = *((pg_crc32c *) (buf + crc_offset));

    if (!EQ_CRC32C(calc_crc, file_crc))
        ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
                       errmsg("calculated CRC checksum does not match value stored in file \"%s\"",
                              path)));

    return buf;
}
```