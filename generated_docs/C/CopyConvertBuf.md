# CopyConvertBuf

## Location
[src/backend/commands/copyfromparse.c:400-532](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfromparse.c#L400-L532)

## Overview
CopyConvertBuf performs character encoding conversion on data from the raw input buffer to the input buffer during COPY FROM operations, handling both cases where conversion is needed and where only validation is required.

## Definition

```c
static void
CopyConvertBuf(CopyFromState cstate)
```
## Detailed Description
CopyConvertBuf is a critical function in PostgreSQL's COPY FROM parsing pipeline that handles character encoding conversion and validation. The function operates in two distinct modes based on whether transcoding is needed:

1. **No transcoding needed**: When file and server encodings match, the function validates that the input is properly encoded using pg_encoding_verifymbstr. It tracks verified vs unverified bytes and reports encoding errors for incomplete or invalid sequences.

2. **Transcoding required**: When encodings differ, it performs actual conversion using pg_do_encoding_conversion_buf, converting from file encoding to database encoding. The conversion is done incrementally, handling incomplete multi-byte sequences gracefully.

The function implements sophisticated error handling, deferring error reporting until all valid data has been processed to avoid premature errors when end-of-input markers (\.) appear before invalid sequences.

## Parameters / Member Variables
- : CopyFromState structure containing all state information for the COPY operation, including buffers, encoding settings, and progress tracking

## Dependencies
- Functions called/Symbols referenced:
  - [pg_encoding_verifymbstr](../p/pg_encoding_verifymbstr.md)
  - [pg_encoding_max_length](../p/pg_encoding_max_length.md)  
  - [pg_do_encoding_conversion_buf](../p/pg_do_encoding_conversion_buf.md)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - MAX_CONVERSION_INPUT_LENGTH
  - [CopyFromState](CopyFromState.md)
- Called from (representative examples):
  - [CopyLoadInputBuf](CopyLoadInputBuf.md)

## Notes and Other Information
- The function handles two buffer management strategies: when no transcoding is needed, input_buf and raw_buf point to the same memory; when transcoding is required, they are separate buffers
- Error detection is conservative - it waits until there's sufficient input or EOF before reporting encoding errors to handle incomplete multi-byte sequences properly
- The function updates various state variables including input_buf_len, raw_buf_index, and sets flags like input_reached_eof and input_reached_error
- Memory management includes moving unprocessed data within buffers to make room for new converted data