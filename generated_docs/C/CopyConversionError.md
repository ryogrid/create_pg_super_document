# CopyConversionError

## Location
[src/backend/commands/copyfromparse.c:533-589](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfromparse.c#L533-L589)

## Overview
CopyConversionError reports encoding or conversion errors encountered during COPY FROM operations, providing appropriate error messages based on whether transcoding was required.

## Definition


## Detailed Description
CopyConversionError is responsible for generating detailed error reports when character encoding problems are detected during COPY FROM processing. The function operates differently depending on whether encoding conversion was needed:

1. **No transcoding case**: When file and database encodings are the same, it uses report_invalid_encoding() to generate an error message about the invalid or incomplete character sequence found during validation.

2. **Transcoding case**: When encoding conversion is required, it re-invokes the conversion function (pg_do_encoding_conversion_buf) with noError=false to let the conversion routine generate a more specific error message about the untranslatable or invalid character sequence.

The function is designed to be called only after CopyConvertBuf has detected an encoding problem (indicated by input_reached_error flag), ensuring that error reporting happens at the right time in the processing pipeline.

## Parameters / Member Variables
- : CopyFromState structure containing the COPY operation state, including buffer positions, encoding settings, and error flags

## Dependencies
- Functions called/Symbols referenced:
  - [report_invalid_encoding](../r/report_invalid_encoding.md)
  - [pg_do_encoding_conversion_buf](../p/pg_do_encoding_conversion_buf.md)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - CopyFromState
- Called from (representative examples):
  - [CopyLoadInputBuf](CopyLoadInputBuf.md)

## Notes and Other Information
- The function includes assertions to verify that there is data in the raw buffer and that an error was actually detected
- In the transcoding case, the function calls pg_do_encoding_conversion_buf with noError=false specifically to trigger the error - this is a deliberate design to get detailed error messages from the conversion subsystem
- The fallback elog(ERROR) should never be reached as the conversion routine is expected to throw an error when noError=false
- Error positioning is precise: input_buf_len points to the problematic character when no transcoding is needed, while raw_buf_index points to it when transcoding is required