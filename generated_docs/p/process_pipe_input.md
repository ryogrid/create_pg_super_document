# process_pipe_input

## Location
[src/backend/postmaster/syslogger.c:880-1042](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/syslogger.c#L880-L1042)

## Overview
process_pipe_input processes data received through the syslogger pipe, implementing a chunked protocol to reassemble log messages from multiple backends while avoiding partial writes and message interleaving.

## Definition

```c
static void
process_pipe_input(char *logbuffer, int *bytes_in_logbuffer)
```
## Detailed Description
process_pipe_input is the core message processing function in PostgreSQL's logging system. It implements a sophisticated protocol for handling log data received from multiple backend processes through a shared pipe. The function addresses two critical logging problems:

1. **Preventing partial messages**: Ensures that log entries are written atomically to log files, preventing log rotation from splitting messages
2. **Avoiding message interleaving**: Prevents log messages from different backends from being mixed together

The protocol uses a header structure (PipeProtoHeader) that includes:
- Two null bytes as a signature
- 16-bit message length
- Process ID of the sender
- Flags indicating destination (stderr/CSV/JSON) and whether this is the final chunk

**Processing Logic:**
- **Protocol Messages**: Messages with valid headers are processed according to the chunk protocol. Non-final chunks are buffered per process ID, while final chunks complete the message and trigger output.
- **Non-Protocol Messages**: Data that doesn't match the protocol (e.g., from third-party libraries) is written directly to stderr logs.
- **Buffer Management**: Uses hash tables (buffer_lists) indexed by PID to efficiently manage partial messages from multiple processes.

## Parameters / Member Variables
- : Input buffer containing received log data
- : Pointer to the number of bytes currently in the buffer (updated on exit to reflect consumed data)

## Dependencies
- Functions called/Symbols referenced:
  - [write_syslogger_file](../w/write_syslogger_file.md) (outputs complete messages to appropriate log files)
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md) (accumulates partial message chunks)
  - [initStringInfo](../i/initStringInfo.md) (initializes string buffers for new messages)
  - [PipeProtoHeader](../P/PipeProtoHeader.md) (protocol header structure)
  - PIPE_PROTO_DEST_* constants (destination flags)
  - LOG_DESTINATION_* constants (log file destinations)
- Called from (representative examples):
  - [SysLoggerMain](../S/SysLoggerMain.md) (main Unix/Linux processing loop)
  - [pipeThread](pipeThread.md) (Windows background thread)

## Notes and Other Information
- Implements a sophisticated buffering system using hash tables to track partial messages from different processes
- Handles both protocol and non-protocol data gracefully, ensuring no log data is lost
- Uses efficient left-alignment of remaining buffer data to minimize memory copying
- The protocol supports multiple log destinations (stderr, CSV, JSON) through flag bits
- Buffer management includes slot reuse to minimize memory allocation overhead
- Critical for PostgreSQL's ability to maintain clean, ordered logs in multi-process environments
- The function is designed to handle incomplete reads and partial headers, making it robust against network and pipe timing issues

## Simplified Source

```c
// Simplified version of process_pipe_input
static void process_pipe_input(char *logbuffer, int *bytes_in_logbuffer) {
    char *cursor = logbuffer;
    int count = *bytes_in_logbuffer;
    int dest = LOG_DESTINATION_STDERR;

    // Main processing loop: handle complete chunks
    while (count >= PIPE_HEADER_SIZE) {
        PipeProtoHeader header;
        memcpy(&header, cursor, sizeof(header));

        // Check if this is a valid protocol message
        if (is_valid_protocol_header(&header)) {
            int chunk_size = PIPE_HEADER_SIZE + header.len;

            // Skip if we don't have the complete chunk yet
            if (count < chunk_size) break;

            // Determine log destination from header flags
            dest = get_log_destination(header.flags);

            // Handle message buffering and assembly
            if (header.flags & PIPE_PROTO_IS_LAST) {
                // Final chunk: complete the message and write it out
                complete_and_write_message(header.pid, cursor, header.len, dest);
            } else {
                // Non-final chunk: buffer it for later assembly
                buffer_partial_message(header.pid, cursor + PIPE_HEADER_SIZE, header.len);
            }

            // Move to next chunk
            cursor += chunk_size;
            count -= chunk_size;
        } else {
            // Non-protocol data: find next potential header or write everything
            int non_protocol_len = find_next_header_or_end(cursor, count);
            write_syslogger_file(cursor, non_protocol_len, LOG_DESTINATION_STDERR);
            cursor += non_protocol_len;
            count -= non_protocol_len;
        }
    }

    // Compact remaining data to start of buffer
    if (count > 0 && cursor != logbuffer) {
        memmove(logbuffer, cursor, count);
    }
    *bytes_in_logbuffer = count;
}
```

Key simplifications made:
- Abstracted complex header validation into `is_valid_protocol_header()`
- Simplified destination determination with `get_log_destination()`
- Consolidated message assembly logic into `complete_and_write_message()` and `buffer_partial_message()`
- Removed detailed buffer list management implementation details
- Abstracted non-protocol data scanning into `find_next_header_or_end()`
- Focused on the main algorithm flow: validate headers, buffer partial chunks, complete messages
- Maintained the essential chunked protocol logic while removing low-level memory management details