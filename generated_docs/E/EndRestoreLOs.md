# EndRestoreLOs

## Location
src/bin/pg_dump/pg_backup_archiver.c: 1449 - 1471

## Overview
Finalizes the restoration process for a group of Large Objects by committing the transaction if needed and logging the completion status.

## Definition
```c
void EndRestoreLOs(ArchiveHandle *AH)
```

## Detailed Description
This function is called by format handlers after completing the restoration of a group of Large Objects. It commits the transaction that was potentially started by StartRestoreLOs, ensuring all LO restoration changes are properly persisted. The function also provides user feedback by logging the total number of Large Objects that were successfully restored, using proper pluralization for the message.

## Parameters / Member Variables
- `AH`: Archive handle containing restoration context and the LO counter

## Dependencies
- Functions called/Symbols referenced:
  - RestoreOptions
  - CommitTransaction
  - ahprintf
  - ngettext
  - pg_log_info
- Called from (representative examples):
  - _LoadLOs (in pg_backup_custom.c, pg_backup_directory.c, pg_backup_tar.c)

## Notes and Other Information
- Only commits the transaction if it was started by StartRestoreLOs (respects single_txn and txn_size settings)
- Uses ngettext for proper pluralization of the completion message ("object" vs "objects")
- Works with both connected (database connection) and disconnected (script output) restoration modes
- The loCount field tracks the total number of LOs restored during the session