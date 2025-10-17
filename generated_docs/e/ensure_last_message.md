# ensure_last_message

## Location
[src/backend/replication/logical/worker.c:1971-2002](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/worker.c#L1971-L2002)

## Overview
ensure_last_message is a static function that validates that a given file position represents the end of a streaming transaction changes file, ensuring data integrity in PostgreSQL logical replication.

## Definition
```c
static void ensure_last_message(FileSet *stream_fileset, TransactionId xid, int fileno, off_t offset)
```

## Detailed Description
This function performs a critical validation step in PostgreSQL logical replication by verifying that a specified position (fileno and offset) corresponds to the actual end of a streaming transaction changes file. It opens the changes file associated with the given transaction ID, seeks to the end, and compares the actual end position with the expected position parameters. If there is a mismatch, it raises an error indicating unexpected data remains in the file.

The function operates within replication steps (begin_replication_step/end_replication_step) and ensures it runs outside of transaction state, as indicated by the IsTransactionState() assertion.

## Parameters / Member Variables
- `stream_fileset`: FileSet containing the streaming transaction changes files
- `xid`: Transaction ID used to identify the specific changes file  
- `fileno`: Expected file number at the end position
- `offset`: Expected offset within the file at the end position

## Dependencies
- Functions called/Symbols referenced:
  - [IsTransactionState](../I/IsTransactionState.md) (assertion check)
  - [begin_replication_step](../b/begin_replication_step.md)
  - [changes_filename](../c/changes_filename.md) (constructs file path)
  - [BufFileOpenFileSet](../B/BufFileOpenFileSet.md)
  - [BufFileSeek](../B/BufFileSeek.md)
  - [BufFileTell](../B/BufFileTell.md)  
  - [BufFileClose](../B/BufFileClose.md)
  - [end_replication_step](end_replication_step.md)
- Called from (representative examples):
  - [apply_spooled_messages](../a/apply_spooled_messages.md)

## Notes and Other Information
- This is a static function used internally within the logical replication worker
- Contains an assertion that ensures it runs outside transaction state
- Raises an ERROR if validation fails, indicating potential data corruption or incomplete processing
- Part of PostgreSQL streaming replication infrastructure for handling large transactions
- File path construction uses MyLogicalRepWorker->subid to identify the subscription context

## Simplified Source

```c
static void ensure_last_message(FileSet *stream_fileset, TransactionId xid,
                               int fileno, off_t offset)
{
    char path[MAXPGPATH];
    BufFile *fd;
    int last_fileno;
    off_t last_offset;

    // Must not be in transaction state
    Assert(!IsTransactionState());

    begin_replication_step();

    // Construct file path for the changes file
    changes_filename(path, MyLogicalRepWorker->subid, xid);

    // Open file and seek to end
    fd = BufFileOpenFileSet(stream_fileset, path, O_RDONLY, false);
    BufFileSeek(fd, 0, 0, SEEK_END);
    BufFileTell(fd, &last_fileno, &last_offset);
    BufFileClose(fd);

    end_replication_step();

    // Validate that we're at the expected end position
    if (last_fileno != fileno || last_offset != offset)
        elog(ERROR, "unexpected message left in streaming transaction's changes file \"%s\"",
             path);
}
```