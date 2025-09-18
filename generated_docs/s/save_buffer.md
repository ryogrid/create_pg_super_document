# save_buffer

## Location
src/backend/postmaster/syslogger.c: 109 - 110

## Overview
A structure used by PostgreSQL's system logger to temporarily buffer log messages from multiple processes before writing them to log files.

## Definition


## Detailed Description
The  structure is a core component of PostgreSQL's system logger (syslogger) mechanism, defined in . It serves as a temporary storage buffer for log messages that arrive in multiple chunks from various PostgreSQL processes. 

The syslogger uses these buffers to reassemble fragmented log messages before writing them to the appropriate log files. When a log message is too large to fit in a single pipe transmission, it gets split into multiple chunks. The  allows the syslogger to collect all chunks belonging to a single message from a specific process and reconstruct the complete message.

The buffer management system uses a hash-table-like approach with  (256) lists to distribute buffers based on process ID. Each buffer is associated with a specific source process via its PID, and inactive buffers (with ) are kept in the lists for reuse rather than being deallocated.

## Parameters / Member Variables
- : The process ID of the source process that generated the log data. When set to 0, indicates an inactive/unused buffer that can be reused for new messages.
- : A StringInfoData structure that accumulates the actual log message content as it arrives in chunks from the source process.

## Dependencies
- Functions called/Symbols referenced:
  - [StringInfoData](../S/StringInfoData.md) (for data member)
  - int32 (for pid member)

- Called from (representative examples):
  - [process_pipe_input](../p/process_pipe_input.md) (allocates and manages save_buffer instances)
  - [flush_pipe_input](../f/flush_pipe_input.md) (iterates through save_buffer instances to flush remaining data)

## Notes and Other Information
- Buffers are organized in 256 hash buckets () to distribute load based on PID
- Inactive buffers are not removed from lists but marked with  for efficient reuse
- Each buffer corresponds to exactly one source process - there can never be more than one buffer for the same PID in the system
- The structure is specifically designed for the syslogger's message reassembly process where log output from backends may arrive fragmented across multiple pipe reads
- Memory for the StringInfoData is managed automatically through PostgreSQL's memory context system