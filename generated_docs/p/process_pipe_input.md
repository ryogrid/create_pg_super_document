# process_pipe_input

## Location
src/backend/postmaster/syslogger.c: 880 - 1042

## Overview
process_pipe_input processes data received through the syslogger pipe, implementing a chunked protocol to reassemble log messages from multiple backends while avoiding partial writes and message interleaving.

## Definition


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
  - appendBinaryStringInfo (accumulates partial message chunks)
  - initStringInfo (initializes string buffers for new messages)
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