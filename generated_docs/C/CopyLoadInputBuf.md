# CopyLoadInputBuf

## Location
[src/backend/commands/copyfromparse.c:650-700](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfromparse.c#L650-L700)

## Overview
CopyLoadInputBuf ensures that input_buf contains converted, ready-to-process data by orchestrating the loading of raw data, encoding conversion, and error handling for COPY FROM operations.

## Definition

```c
static void
CopyLoadInputBuf(CopyFromState cstate)
```
## Detailed Description
CopyLoadInputBuf serves as the central coordinator for the COPY FROM input pipeline, ensuring that processed input data is available for parsing. The function implements a sophisticated loop that:

1. **Manages buffer synchronization**: When raw_buf and input_buf are the same (no transcoding needed), it keeps the buffer indices synchronized.

2. **Orchestrates conversion**: Calls CopyConvertBuf() to convert raw data into the proper encoding and validate character sequences.

3. **Handles encoding errors**: Detects when CopyConvertBuf() encounters invalid byte sequences and delegates error reporting to CopyConversionError().

4. **Manages data flow**: Continuously loads new raw data via CopyLoadRawBuf() until either sufficient converted data is available or EOF is reached.

The function guarantees that on successful return, at least one new character is available in input_buf, or input_reached_eof is set if no more data can be processed.

## Parameters / Member Variables
- : CopyFromState structure containing all COPY operation state, including buffer management variables, encoding flags, and progress tracking

## Dependencies
- Functions called/Symbols referenced:
  - [CopyConvertBuf](CopyConvertBuf.md)
  - [CopyConversionError](CopyConversionError.md)  
  - [CopyLoadRawBuf](CopyLoadRawBuf.md)
  - [CopyFromState](CopyFromState.md)
- Called from (representative examples):
  - NO_END_OF_COPY_GOTO
  - [CopyReadLineText](CopyReadLineText.md)

## Notes and Other Information
- The function implements a loop that continues until either new input data becomes available or an error/EOF condition is reached
- Buffer index synchronization ensures consistency when raw_buf and input_buf point to the same memory area
- The function includes assertions to verify expected state relationships, particularly when no transcoding is required
- Error handling is deferred to specialized functions (CopyConversionError) to provide detailed encoding error messages
- The INPUT_BUF_BYTES macro is used to efficiently check available processed data
- The function guarantees progress by ensuring that either new data becomes available or a terminal condition (EOF/error) is reached

## Simplified Source

```c
static void
CopyLoadInputBuf(CopyFromState cstate)
{
    int nbytes = INPUT_BUF_BYTES(cstate);

    // Synchronize buffer indices when raw_buf and input_buf are the same
    if (cstate->raw_buf == cstate->input_buf)
    {
        Assert(!cstate->need_transcoding);
        Assert(cstate->input_buf_index >= cstate->raw_buf_index);
        cstate->raw_buf_index = cstate->input_buf_index;
    }

    for (;;)
    {
        // Convert any available raw data to proper encoding
        CopyConvertBuf(cstate);

        // Check if we have new input bytes ready to return
        if (INPUT_BUF_BYTES(cstate) > nbytes)
            return;

        // Handle encoding conversion errors
        if (cstate->input_reached_error)
            CopyConversionError(cstate);

        // Exit if we've reached EOF and processed all data
        if (cstate->input_reached_eof)
            break;

        // Load more raw data if available
        Assert(!cstate->raw_reached_eof);
        CopyLoadRawBuf(cstate);
    }
}
```