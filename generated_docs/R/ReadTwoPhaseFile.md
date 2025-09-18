# ReadTwoPhaseFile

## Location
src/backend/access/transam/twophase.c: 1287 - 1403

## Overview
ReadTwoPhaseFile reads and validates a two-phase commit state file from disk, performing integrity checks before returning the file contents.

## Definition


## Detailed Description
ReadTwoPhaseFile is responsible for securely reading two-phase commit state files from the filesystem and validating their integrity. It constructs the file path using the transaction ID, opens the file with proper error handling, validates file size constraints, reads the entire file contents into memory, and performs comprehensive validation including magic number verification and CRC checksum validation. The function supports a missing_ok parameter to handle recovery scenarios where files may legitimately not exist.

## Parameters / Member Variables
- : TransactionId of the prepared transaction whose state file should be read
- : bool flag indicating whether missing files should return NULL instead of throwing an error (used during recovery)

## Dependencies
- Functions called/Symbols referenced:
  - TwoPhaseFilePath
  - OpenTransientFile
  - fstat
  - pgstat_report_wait_start
  - pgstat_report_wait_end
  - CloseTransientFile
  - INIT_CRC32C
  - COMP_CRC32C
  - FIN_CRC32C
  - EQ_CRC32C
- Called from (representative examples):
  - StandbyTransactionIdIsPrepared
  - FinishPreparedTransaction
  - ProcessTwoPhaseBuffer
  - LookupGXact

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