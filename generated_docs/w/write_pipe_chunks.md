# write_pipe_chunks

## Location
src/backend/utils/error/elog.c: 3426 - 3476

## Overview
Safely transmits log data to the syslogger process using a chunked protocol that ensures atomic writes and prevents message interleaving.

## Definition
```c
void write_pipe_chunks(char *data, int len, int dest)
```

## Detailed Description
The `write_pipe_chunks` function implements a reliable communication protocol for sending log messages from backend processes to the syslogger process through stderr pipes. It addresses the critical requirement that writes to pipes must be atomic to prevent interleaving of data from multiple concurrent processes.

The function splits large messages into chunks that respect the PIPE_BUF size limit (ensuring atomic writes per POSIX specification), packages each chunk with protocol headers containing metadata (process ID, destination flags, chunk length), and transmits them sequentially. The syslogger process on the receiving end knows how to reassemble these chunks back into complete messages.

Each chunk includes a PipeProtoChunk header with destination information (stderr, CSV log, or JSON log) and flags indicating whether it's the final chunk of a message.

## Parameters / Member Variables
- `data`: Pointer to the log message data to be transmitted
- `len`: Length of the data in bytes
- `dest`: Destination type (LOG_DESTINATION_STDERR, LOG_DESTINATION_CSVLOG, or LOG_DESTINATION_JSONLOG)

## Dependencies
- Functions called/Symbols referenced:
  - [PipeProtoChunk](../P/PipeProtoChunk.md) (struct type)
  - write (system call)
  - Various pipe protocol constants (PIPE_MAX_PAYLOAD, PIPE_HEADER_SIZE, PIPE_PROTO_IS_LAST, etc.)
  - Log destination constants
- Called from:
  - [send_message_to_server_log](../s/send_message_to_server_log.md)
  - [write_csvlog](write_csvlog.md)
  - [write_jsonlog](write_jsonlog.md)

## Notes and Other Information
- Critical for preventing log message corruption in multi-process environments
- Relies on POSIX atomic write guarantees for pipes when writes are ≤ PIPE_BUF bytes
- Error handling is minimal by design - write failures are ignored since there's no alternative destination
- The function deliberately ignores write() return values with void casting to suppress compiler warnings
- Essential component of PostgreSQL's logging infrastructure when stderr redirection is active
- Uses stderr file descriptor (fileno(stderr)) for all communication with syslogger