# PipeProtoChunk

## Location
[src/include/postmaster/syslogger.h:57-58](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postmaster/syslogger.h#L57-L58)

## Overview
PipeProtoChunk is a union type that provides a memory layout for pipe protocol chunks in PostgreSQL's system logger, combining the protocol header with a fixed-size buffer for efficient memory management.

## Definition

```c
typedef union
{
	PipeProtoHeader proto;
	char		filler[PIPE_CHUNK_SIZE];
} PipeProtoChunk;
```
## Detailed Description
PipeProtoChunk is a union that serves as a memory management abstraction for the pipe protocol used in PostgreSQL's logging system. It provides two different views of the same memory space: one as a structured protocol header (PipeProtoHeader) and another as a raw character buffer of fixed size (PIPE_CHUNK_SIZE). This design allows the system to efficiently allocate memory for log chunks while ensuring proper alignment and size constraints.

The union ensures that each chunk allocation is exactly PIPE_CHUNK_SIZE bytes, providing predictable memory usage and simplifying buffer management in the logging subsystem. The filler array guarantees that the union is large enough to hold the maximum possible chunk size, while the proto member provides structured access to the header and data payload.

## Parameters / Member Variables
- : PipeProtoHeader structure that provides structured access to the protocol header fields (nuls, len, pid, flags) and data payload
- : Character array of PIPE_CHUNK_SIZE bytes that ensures the union has the correct size and provides raw buffer access

## Dependencies
- Functions called/Symbols referenced:
  - [PipeProtoHeader](PipeProtoHeader.md) (the structured protocol header type)
  - PIPE_CHUNK_SIZE (macro defining the total chunk size)
- Called from (representative examples):
  - [write_pipe_chunks](../w/write_pipe_chunks.md) (in src/backend/utils/error/elog.c)

## Notes and Other Information
- The union design ensures efficient memory usage while maintaining proper alignment
- PIPE_CHUNK_SIZE is platform-dependent, typically 65536 bytes on systems with large pipe buffers, or PIPE_BUF on others
- Used primarily in write_pipe_chunks function to send log data through pipes
- The union guarantees that protocol chunks have a consistent size regardless of the actual data payload length
- Memory allocated as PipeProtoChunk can be safely cast between the structured and raw buffer views
- Essential for the chunked transmission protocol that allows large log messages to be split across multiple pipe writes