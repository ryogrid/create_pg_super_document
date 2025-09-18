# ValidXLogRecordHeader

## Location
src/backend/access/transam/xlogreader.c: 1137 - 1202

## Overview
ValidXLogRecordHeader validates the header portion of an XLOG (Write-Ahead Log) record to ensure data integrity and structural consistency during WAL record processing.

## Definition


## Detailed Description
ValidXLogRecordHeader is a static validation function specifically designed as a convenience subroutine to avoid code duplication in XLogReadRecord. It performs comprehensive validation of XLOG record headers by checking:

1. **Record length validation**: Ensures the record's total length meets the minimum requirement (at least SizeOfXLogRecord bytes)
2. **Resource manager ID validation**: Verifies the resource manager ID is within valid bounds using RmgrIdIsValid
3. **Previous link validation**: Validates the prev-link field based on access mode:
   - For random access: Ensures prev-link is less than the current record pointer
   - For sequential access: Ensures prev-link exactly matches the expected previous record pointer to guard against torn WAL pages

The function uses different validation strategies depending on whether random access or sequential access is being performed, providing protection against various types of WAL corruption scenarios.

## Parameters / Member Variables
- : XLogReaderState pointer containing the current reader state and error reporting context
- : XLogRecPtr specifying the current record's location in the WAL
- : XLogRecPtr indicating the expected location of the previous record
- : XLogRecord pointer to the record header being validated
- : Boolean flag indicating whether random access mode is being used (affects prev-link validation logic)

## Dependencies
- Functions called/Symbols referenced:
  - SizeOfXLogRecord (constant for minimum record size)
  - report_invalid_record (error reporting function)
  - RmgrIdIsValid (resource manager ID validation function)
  - XLogRecord (struct type being validated)
- Called from (representative examples):
  - XLogDecodeNextRecord (at lines 658 and 814)

## Notes and Other Information
- This is a static function not intended for use outside of xlogreader.c
- The function provides different validation behavior for random vs sequential access patterns
- Critical for preventing acceptance of corrupted or torn WAL pages
- Returns boolean value indicating validation success/failure
- Error details are reported through the report_invalid_record function when validation fails