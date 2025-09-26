# internal_flush

## Location
[src/backend/libpq/pqcomm.c:1346-1358](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/libpq/pqcomm.c#L1346-L1358)

## Overview
A static inline function that flushes the PostgreSQL send buffer by delegating to the core buffer flushing implementation.

## Definition

```c
static inline int
internal_flush(void)
```
## Detailed Description
The  function serves as a simple wrapper around  for flushing PostgreSQL's global send buffer. It provides a convenient interface for flushing the standard send buffer without requiring callers to manage buffer state variables directly.

This function is designed to handle both blocking and non-blocking socket modes gracefully. In non-blocking mode, it returns success even if not all data could be sent immediately (allowing the operation to be retried later), while in blocking mode it ensures all buffered data is transmitted before returning.

The function operates on PostgreSQL's global send buffer state variables (, , and ) and is inlined for performance since it's called frequently during message transmission.

## Parameters / Member Variables
- No parameters (operates on global send buffer state)

## Dependencies
- Functions called/Symbols referenced:
  - [internal_flush_buffer](internal_flush_buffer.md) (core buffer flushing implementation)
- Called from (representative examples):
  - [internal_putbytes](internal_putbytes.md) (when send buffer becomes full)
  - [socket_flush](../s/socket_flush.md) (public flush interface)
  - [socket_flush_if_writable](../s/socket_flush_if_writable.md) (conditional flushing)

## Notes and Other Information
- Function is marked as static inline for performance optimization
- Returns 0 on success or when operation would block in non-blocking mode
- Returns EOF on transmission errors
- Operates on global buffer variables: PqSendBuffer, PqSendStart, PqSendPointer
- Part of PostgreSQL's layered communication architecture
- Provides abstraction layer between high-level flush operations and low-level buffer management
- Critical component for maintaining message transmission reliability in client-server communication