# PipeProtoHeader

## Location
src/include/postmaster/syslogger.h: 51 - 56

## Overview
PipeProtoHeader is a structure that defines the header format for pipe protocol communication in PostgreSQL's system logger, providing metadata for chunks of log data transmitted through pipes.

## Definition


## Detailed Description
PipeProtoHeader serves as the header structure for the pipe protocol used by PostgreSQL's system logger (syslogger) to communicate log data between processes. This structure is designed to facilitate reliable transmission of log messages through pipes by providing essential metadata about each data chunk. The header includes synchronization markers, size information, process identification, and control flags to ensure proper parsing and handling of log data streams.

The structure uses a flexible array member for the data payload, allowing variable-length messages while maintaining a fixed header size. This design enables efficient memory usage and supports chunked transmission of large log messages.

## Parameters / Member Variables
- : Two null bytes that serve as synchronization markers, always set to \0\0 to help identify valid protocol headers
- : 16-bit unsigned integer specifying the size of the data payload in this chunk (excludes header size)
- : 32-bit signed integer containing the process ID of the writer that generated this log message
- : 8-bit bitmask containing protocol control flags, including PIPE_PROTO_IS_LAST to indicate the final chunk of a message
- : Flexible array member that contains the actual log data payload

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (macro for flexible array syntax)
  - PIPE_CHUNK_SIZE (defines the total chunk size including header)
- Called from (representative examples):
  - [process_pipe_input](../p/process_pipe_input.md) (in src/backend/postmaster/syslogger.c)
  - PIPE_HEADER_SIZE (macro using offsetof with this structure)

## Notes and Other Information
- The structure is part of the pipe protocol implementation for PostgreSQL's logging system
- Used in conjunction with PipeProtoChunk union for memory management
- The header size is calculated using PIPE_HEADER_SIZE macro (offsetof(PipeProtoHeader, data))
- Maximum payload size is determined by PIPE_MAX_PAYLOAD (PIPE_CHUNK_SIZE - PIPE_HEADER_SIZE)
- The nuls field provides a simple way to detect protocol synchronization issues
- Supports chunked transmission for large log messages that exceed the pipe buffer size