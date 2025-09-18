# ValidXLogRecord

## Location
src/backend/access/transam/xlogreader.c: 1203 - 1233

## Overview
ValidXLogRecord performs CRC (Cyclic Redundancy Check) validation on an XLOG record to verify data integrity before trusting the record contents.

## Definition
```c
static bool ValidXLogRecord(XLogReaderState *state, XLogRecord *record, XLogRecPtr recptr)
```

## Detailed Description
ValidXLogRecord is a critical data integrity function that performs CRC-based validation of XLOG records. The function operates under the assumption that the entire record (xl_tot_len bytes) has been read into memory and that ValidXLogRecordHeader() has already accepted the record's header.

The CRC calculation process follows a specific sequence:
1. **Initialize CRC**: Sets up the CRC32C calculation context
2. **Calculate data CRC**: Computes CRC for the record data portion (excluding the header and CRC field)
3. **Include header**: Adds the record header to the CRC calculation, excluding the xl_crc field itself
4. **Finalize CRC**: Completes the CRC calculation
5. **Compare**: Verifies the calculated CRC matches the stored CRC in the record

This validation is essential because PostgreSQL does not trust XLOG record contents until CRC verification is complete, providing protection against data corruption during WAL processing.

## Parameters / Member Variables
- `state`: XLogReaderState pointer used for error reporting context
- `record`: XLogRecord pointer to the complete record in memory that needs CRC validation
- `recptr`: XLogRecPtr specifying the record's location in the WAL (used for error reporting)

## Dependencies
- Functions called/Symbols referenced:
  - SizeOfXLogRecord (constant for record header size)
  - pg_crc32c (CRC data type)
  - INIT_CRC32C (CRC initialization macro)
  - COMP_CRC32C (CRC computation macro)
  - FIN_CRC32C (CRC finalization macro)
  - EQ_CRC32C (CRC comparison macro)
  - [report_invalid_record](../r/report_invalid_record.md) (error reporting function)
  - [XLogRecord](../X/XLogRecord.md) (struct type being validated)
- Called from (representative examples):
  - [XLogDecodeNextRecord](../X/XLogDecodeNextRecord.md) (at lines 845 and 864)

## Notes and Other Information
- Static function not intended for external use outside xlogreader.c
- Assumes ValidXLogRecordHeader() has already validated the record header
- Uses CRC32C algorithm for integrity checking
- The CRC calculation specifically excludes the xl_crc field itself to avoid circular dependency
- Critical for data integrity - PostgreSQL will not trust record contents without successful CRC validation
- Returns boolean indicating whether CRC validation passed or failed