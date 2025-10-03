# ValidXLogRecordHeader

## Location
[src/backend/access/transam/xlogreader.c:1137-1202](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlogreader.c#L1137-L1202)

## Overview
ValidXLogRecordHeader validates the header portion of an XLOG (Write-Ahead Log) record to ensure data integrity and structural consistency during WAL record processing.

## Definition

```c
static bool
ValidXLogRecordHeader(XLogReaderState *state, XLogRecPtr RecPtr,
					  XLogRecPtr PrevRecPtr, XLogRecord *record,
					  bool randAccess)
```
## Detailed Description
ValidXLogRecordHeader is a static validation function specifically designed as a convenience subroutine to avoid code duplication in XLogReadRecord. It performs comprehensive validation of XLOG record headers by checking:

1. **Record length validation**: Ensures the record's total length meets the minimum requirement (at least SizeOfXLogRecord bytes)
2. **Resource manager ID validation**: Verifies the resource manager ID is within valid bounds using RmgrIdIsValid
3. **Previous link validation**: Validates the prev-link field based on access mode:
   - For random access: Ensures prev-link is less than the current record pointer
   - For sequential access: Ensures prev-link exactly matches the expected previous record pointer to guard against torn WAL pages

The function uses different validation strategies depending on whether random access or sequential access is being performed, providing protection against various types of WAL corruption scenarios.

## Parameters / Member Variables
- `*state`: XLogReaderState pointer containing the current reader state and error reporting context
- `RecPtr`: XLogRecPtr specifying the current record's location in the WAL
- `PrevRecPtr`: XLogRecPtr indicating the expected location of the previous record
- `*record`: XLogRecord pointer to the record header being validated
- `randAccess`: Boolean flag indicating whether random access mode is being used (affects prev-link validation logic)
## Dependencies
- Functions called/Symbols referenced:
  - SizeOfXLogRecord (constant for minimum record size)
  - [report_invalid_record](../r/report_invalid_record.md) (error reporting function)
  - RmgrIdIsValid (resource manager ID validation function)
  - [XLogRecord](../X/XLogRecord.md) (struct type being validated)
- Called from (representative examples):
  - [XLogDecodeNextRecord](../X/XLogDecodeNextRecord.md) (at lines 658 and 814)

## Notes and Other Information
- This is a static function not intended for use outside of xlogreader.c
- The function provides different validation behavior for random vs sequential access patterns
- Critical for preventing acceptance of corrupted or torn WAL pages
- Returns boolean value indicating validation success/failure
- Error details are reported through the report_invalid_record function when validation fails

## Simplified Source

```c
static bool ValidXLogRecordHeader(XLogReaderState *state, XLogRecPtr RecPtr,
                                  XLogRecPtr PrevRecPtr, XLogRecord *record,
                                  bool randAccess) {

    // Check minimum record length
    if (record->xl_tot_len < SizeOfXLogRecord) {
        report_invalid_record(state,
                             "invalid record length at %X/%X: expected at least %u, got %u",
                             LSN_FORMAT_ARGS(RecPtr),
                             (uint32) SizeOfXLogRecord, record->xl_tot_len);
        return false;
    }

    // Validate resource manager ID
    if (!RmgrIdIsValid(record->xl_rmid)) {
        report_invalid_record(state,
                             "invalid resource manager ID %u at %X/%X",
                             record->xl_rmid, LSN_FORMAT_ARGS(RecPtr));
        return false;
    }

    // Validate previous link based on access mode
    if (randAccess) {
        // For random access: prev-link should be less than current record
        if (!(record->xl_prev < RecPtr)) {
            report_invalid_record(state,
                                 "record with incorrect prev-link %X/%X at %X/%X",
                                 LSN_FORMAT_ARGS(record->xl_prev),
                                 LSN_FORMAT_ARGS(RecPtr));
            return false;
        }
    } else {
        // For sequential access: prev-link must match exactly
        if (record->xl_prev != PrevRecPtr) {
            report_invalid_record(state,
                                 "record with incorrect prev-link %X/%X at %X/%X",
                                 LSN_FORMAT_ARGS(record->xl_prev),
                                 LSN_FORMAT_ARGS(RecPtr));
            return false;
        }
    }

    return true;
}
```