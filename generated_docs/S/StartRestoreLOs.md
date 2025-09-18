# StartRestoreLOs

## Location
src/bin/pg_dump/pg_backup_archiver.c: 1425 - 1448

## Overview
Initializes the restoration process for a group of Large Objects (LOs) by establishing a transaction context if needed and resetting the LO counter.

## Definition


## Detailed Description
This function is called by format handlers before beginning to restore a group of Large Objects. It ensures that LO restoration occurs within a proper transaction context, which is essential because LO handles must remain open during the restoration process. The function conditionally starts a transaction if one is not already active globally (through single_txn or txn_size options), and initializes the LO counter to track the number of LOs being restored.

## Parameters / Member Variables
- : Archive handle containing restoration context and options

## Dependencies
- Functions called/Symbols referenced:
  - RestoreOptions
  - StartTransaction
  - ahprintf
- Called from (representative examples):
  - _LoadLOs (in pg_backup_custom.c, pg_backup_directory.c, pg_backup_tar.c)

## Notes and Other Information
- LO restoration requires a transaction block because LO handles must stay open during the write process
- The function respects global transaction settings (single_txn, txn_size) to avoid nested transactions
- Resets the loCount field to 0 to begin counting LOs in the current restoration session
- Works with both connected (database connection) and disconnected (script output) restoration modes