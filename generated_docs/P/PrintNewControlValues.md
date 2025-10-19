# PrintNewControlValues

## Location
[src/bin/pg_resetwal/pg_resetwal.c:789-860](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_resetwal/pg_resetwal.c#L789-L860)

## Overview
PrintNewControlValues displays the control file values that will be modified when pg_resetwal performs its reset operation.

## Definition

```c
static void
PrintNewControlValues(void)
```
## Detailed Description
This static function is part of the pg_resetwal utility and is responsible for printing a formatted summary of all the control file values that will be changed during the WAL reset operation. The function conditionally prints various control file parameters based on what the user has requested to modify through command-line options. This provides transparency to the user about exactly what changes will be made before the actual reset occurs.

The function always prints the "First log segment after reset" information, and then conditionally prints other values only if they have been set through command-line options (checked via global variables like set_mxid, set_mxoff, set_oid, etc.).

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [XLogFileName](../X/XLogFileName.md) (to generate WAL segment filename)
  - XidFromFullTransactionId (to extract XID from full transaction ID)
  - EpochFromFullTransactionId (to extract epoch from full transaction ID)
  - MAXFNAMELEN (constant for maximum filename length)

- Called from:
  - [main](../m/main.md) (in pg_resetwal.c at lines 464 and 474)

## Notes and Other Information
- This is a static function local to pg_resetwal.c
- The function uses internationalization macros (_()) for all printed strings
- Output is conditional based on global flags like set_mxid, set_mxoff, set_oid, set_xid, etc.
- Always prints the first log segment filename regardless of other settings
- Provides user-friendly output showing exactly what will change before the actual reset operation occurs
- Part of the pg_resetwal utility's user interface for transparency and confirmation

## Simplified Source

```c
static void PrintNewControlValues(void) {
    char fname[MAXFNAMELEN];

    // Always print header and first log segment info
    printf("\n\nValues to be changed:\n\n");

    XLogFileName(fname, ControlFile.checkPointCopy.ThisTimeLineID,
                 newXlogSegNo, WalSegSz);
    printf("First log segment after reset:        %s\n", fname);

    // Print MultiXact values if being modified
    if (set_mxid != 0) {
        printf("NextMultiXactId:                      %u\n",
               ControlFile.checkPointCopy.nextMulti);
        printf("OldestMultiXid:                       %u\n",
               ControlFile.checkPointCopy.oldestMulti);
        printf("OldestMulti's DB:                     %u\n",
               ControlFile.checkPointCopy.oldestMultiDB);
    }

    // Print MultiXact offset if being modified
    if (set_mxoff != -1) {
        printf("NextMultiOffset:                      %u\n",
               ControlFile.checkPointCopy.nextMultiOffset);
    }

    // Print OID if being modified
    if (set_oid != 0) {
        printf("NextOID:                              %u\n",
               ControlFile.checkPointCopy.nextOid);
    }

    // Print transaction ID values if being modified
    if (set_xid != 0) {
        printf("NextXID:                              %u\n",
               XidFromFullTransactionId(ControlFile.checkPointCopy.nextXid));
        printf("OldestXID:                            %u\n",
               ControlFile.checkPointCopy.oldestXid);
        printf("OldestXID's DB:                       %u\n",
               ControlFile.checkPointCopy.oldestXidDB);
    }

    // Print XID epoch if being modified
    if (set_xid_epoch != -1) {
        printf("NextXID epoch:                        %u\n",
               EpochFromFullTransactionId(ControlFile.checkPointCopy.nextXid));
    }

    // Print commit timestamp XIDs if being modified
    if (set_oldest_commit_ts_xid != 0) {
        printf("oldestCommitTsXid:                    %u\n",
               ControlFile.checkPointCopy.oldestCommitTsXid);
    }
    if (set_newest_commit_ts_xid != 0) {
        printf("newestCommitTsXid:                    %u\n",
               ControlFile.checkPointCopy.newestCommitTsXid);
    }

    // Print WAL segment size if being modified
    if (set_wal_segsize != 0) {
        printf("Bytes per WAL segment:                %u\n",
               ControlFile.xlog_seg_size);
    }
}
```