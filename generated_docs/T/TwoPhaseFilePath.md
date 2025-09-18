# TwoPhaseFilePath

## Location
[src/backend/access/transam/twophase.c:945-972](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/twophase.c#L945-L972)

## Overview
Constructs the filesystem path for a two-phase commit state file based on a transaction ID, using the full transaction ID format to handle epoch wraparound.

## Definition
static inline int TwoPhaseFilePath(char *path, TransactionId xid)

## Detailed Description
This static inline function generates the complete filesystem path for a two-phase commit state file. It converts the given 32-bit TransactionId to a FullTransactionId to handle epoch wraparound correctly, then constructs a filename using both the epoch and XID components in hexadecimal format. The resulting path follows the pattern "TWOPHASE_DIR/EEEEEEEEXXXXXXXX" where E represents epoch digits and X represents XID digits.

The function uses snprintf to safely format the path string and returns the number of characters written, following standard C library conventions.

## Parameters / Member Variables
- `path`: Character buffer to store the constructed file path (should be at least MAXPGPATH in size)
- `xid`: TransactionId of the prepared transaction for which to construct the file path

## Dependencies
- Functions called/Symbols referenced:
  - [AdjustToFullTransactionId](../A/AdjustToFullTransactionId.md)
  - FullTransactionId
  - TWOPHASE_DIR
  - EpochFromFullTransactionId
  - XidFromFullTransactionId
- Called from (representative examples):
  - [ReadTwoPhaseFile](../R/ReadTwoPhaseFile.md)
  - [RemoveTwoPhaseFile](../R/RemoveTwoPhaseFile.md)
  - [RecreateTwoPhaseFile](../R/RecreateTwoPhaseFile.md)
  - [PrepareRedoAdd](../P/PrepareRedoAdd.md)

## Notes and Other Information
- Returns the number of characters written to the path buffer (standard snprintf behavior)
- Uses hexadecimal formatting (08X) to ensure consistent 8-character width for both epoch and XID components
- The resulting filename uniquely identifies prepared transactions even across epoch boundaries
- Critical for persistent storage and recovery of prepared transaction state
- File paths are used for storing prepared transaction state to disk for crash recovery