# report_invalid_record

## Location
src/backend/access/transam/xlogreader.c: 71 - 89

## Overview
This function constructs an error message string in the XLogReaderState error buffer to explain what is wrong with the current WAL record being read.

## Definition
```c
static void report_invalid_record(XLogReaderState *state, const char *fmt, ...)
```

## Detailed Description
`report_invalid_record` is a static utility function used internally within the xlogreader.c module to format and store error messages when invalid WAL records are encountered. The function takes a format string and variable arguments (similar to printf), formats the error message using vsnprintf, and stores it in the reader states error message buffer. It also sets a flag to indicate that an error message has been deferred for later reporting.

## Parameters / Member Variables
- `state`: Pointer to XLogReaderState structure containing the error message buffer and flags
- `fmt`: Format string for the error message (gets translated via gettext)
- `...`: Variable arguments corresponding to format specifiers in fmt

## Dependencies
- Functions called/Symbols referenced:
  - vsnprintf (standard C library function)
  - MAX_ERRORMSG_LEN (constant defining maximum error message length)
- Called from (representative examples):
  - XLogDecodeNextRecord
  - ValidXLogRecordHeader 
  - ValidXLogRecord
  - XLogReaderValidatePageHeader
  - RestoreBlockImage
  - COPY_HEADER_FIELD

## Notes and Other Information
- This is a static function, meaning it can only be called from within the same source file
- Uses gettext translation (_()) on the format string for internationalization
- Sets errormsg_deferred flag to true, indicating the error should be reported later
- The error message is stored in state->errormsg_buf with a maximum length of MAX_ERRORMSG_LEN
- Extensively used throughout WAL record validation and decoding processes to provide detailed error reporting