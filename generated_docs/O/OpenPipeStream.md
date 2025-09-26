# OpenPipeStream

## Location
[src/backend/storage/file/fd.c:2683-2738](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L2683-L2738)

## Overview
OpenPipeStream is PostgreSQL's managed wrapper around the popen() system call, providing automatic file descriptor management and proper SIGPIPE signal handling for pipe operations.

## Definition

```c
FILE *
OpenPipeStream(const char *command, const char *mode)
```
## Detailed Description
OpenPipeStream serves as PostgreSQL's integrated replacement for the standard popen() function, designed to work within PostgreSQL's file descriptor management system. Beyond basic resource management, this function ensures proper signal handling by temporarily restoring default SIGPIPE behavior during pipe creation, which is crucial for correct pipe operation since PostgreSQL normally runs with SIGPIPE ignored.

The function manages file descriptor limits by releasing least-recently-used files when necessary and provides transaction-aware cleanup. All pipes opened through this function are automatically tracked and closed during transaction boundaries, preventing resource leaks. The function also implements retry logic when encountering file descriptor exhaustion.

## Parameters / Member Variables
- : The shell command string to execute via the pipe
- : The pipe mode ("r" for reading from command output, "w" for writing to command input)

## Dependencies
- Functions called/Symbols referenced:
  - DO_DB (debug logging macro)
  - reserveAllocatedDesc (reserves an allocated descriptor slot)
  - ReleaseLruFiles (closes least-recently-used files to free FDs)
  - pqsignal (PostgreSQL's signal handling function)
  - popen (standard C library pipe creation function)
  - GetCurrentSubTransactionId (tracks which subtransaction opened the pipe)
  - ReleaseLruFile (releases a single LRU file when retrying after EMFILE/ENFILE)
  - SIGPIPE, SIG_DFL, SIG_IGN (signal handling constants)
- Called from (representative examples):
  - pg_import_system_collations (collationcmds.c:872)
  - BeginCopyFrom (copyfrom.c:1719)
  - BeginCopyTo (copyto.c:650)
  - run_ssl_passphrase_command (be-secure-common.c:54)

## Notes and Other Information
- This should be the primary interface for creating pipe streams in PostgreSQL, replacing direct popen() calls
- Pipes opened with OpenPipeStream must be closed with ClosePipeStream, not pclose()
- The function temporarily restores default SIGPIPE handling during pipe creation, then restores SIG_IGN
- All pipes are automatically closed at transaction commit/abort for cleanup
- Implements retry logic when encountering EMFILE/ENFILE errors by releasing LRU files
- The function flushes all stdio streams before creating the pipe to ensure proper I/O ordering
- Signal handling ensures that child processes respond appropriately to broken pipes (e.g., early pipe closure)
- Each opened pipe is tracked in the allocatedDescs array with metadata including the creating subtransaction ID