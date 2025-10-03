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
- `cstate`: CopyFromState structure containing all state information for the COPY operation, including buffers, encoding settings, and progress tracking
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

## Simplified Source

```c
static void
CopyConvertBuf(CopyFromState cstate)
{
    if (!cstate->need_transcoding)
    {
        // No conversion needed - just validate encoding
        int preverifiedlen = cstate->input_buf_len;
        int unverifiedlen = cstate->raw_buf_len - cstate->input_buf_len;

        if (unverifiedlen == 0)
        {
            if (cstate->raw_reached_eof)
                cstate->input_reached_eof = true;
            return;
        }

        // Verify the new data
        int nverified = pg_encoding_verifymbstr(cstate->file_encoding,
                                                cstate->raw_buf + preverifiedlen,
                                                unverifiedlen);
        if (nverified == 0)
        {
            // Could not verify anything - check for error conditions
            if (cstate->raw_reached_eof ||
                unverifiedlen >= pg_encoding_max_length(cstate->file_encoding))
                cstate->input_reached_error = true;
            return;
        }
        cstate->input_buf_len += nverified;
    }
    else
    {
        // Encoding conversion required
        if (RAW_BUF_BYTES(cstate) == 0)
        {
            if (cstate->raw_reached_eof)
                cstate->input_reached_eof = true;
            return;
        }

        // Move any unprocessed data to start of buffer
        int nbytes = INPUT_BUF_BYTES(cstate);
        if (nbytes > 0 && cstate->input_buf_index > 0)
            memmove(cstate->input_buf,
                    cstate->input_buf + cstate->input_buf_index, nbytes);

        cstate->input_buf_index = 0;
        cstate->input_buf_len = nbytes;
        cstate->input_buf[nbytes] = '\0';

        // Set up conversion parameters
        unsigned char *src = (unsigned char *) cstate->raw_buf + cstate->raw_buf_index;
        int srclen = cstate->raw_buf_len - cstate->raw_buf_index;
        unsigned char *dst = (unsigned char *) cstate->input_buf + cstate->input_buf_len;
        int dstlen = INPUT_BUF_SIZE - cstate->input_buf_len + 1;

        // Perform encoding conversion
        int convertedlen = pg_do_encoding_conversion_buf(cstate->conversion_proc,
                                                         cstate->file_encoding,
                                                         GetDatabaseEncoding(),
                                                         src, srclen, dst, dstlen, true);
        if (convertedlen == 0)
        {
            // Could not convert anything - check for error conditions
            if (cstate->raw_reached_eof || srclen >= MAX_CONVERSION_INPUT_LENGTH)
                cstate->input_reached_error = true;
            return;
        }

        // Update buffer positions
        cstate->raw_buf_index += convertedlen;
        cstate->input_buf_len += strlen((char *) dst);
    }
}
```