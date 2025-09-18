# ExtendBufferedRelCommon

## Location
[src/backend/storage/buffer/bufmgr.c:2135-2178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/bufmgr.c#L2135-L2178)

## Overview
ExtendBufferedRelCommon provides shared logic for relation extension operations, routing between temporary and shared buffer handling while managing tracing and relation persistence.

## Definition


## Detailed Description
ExtendBufferedRelCommon serves as a central dispatcher for relation extension operations in PostgreSQL's buffer manager. It determines whether a relation is temporary or shared and delegates to the appropriate extension function (ExtendBufferedRelLocal for temporary relations, ExtendBufferedRelShared for persistent relations). 

The function provides consistent tracing infrastructure using TRACE_POSTGRESQL_BUFFER_EXTEND_START and TRACE_POSTGRESQL_BUFFER_EXTEND_DONE probes, enabling performance monitoring and debugging of buffer extension operations. It abstracts the complexity of different relation types while maintaining a uniform interface for callers.

## Parameters / Member Variables
- : BufferManagerRelation containing relation metadata and storage manager information
- : ForkNumber specifying which fork of the relation to extend (main, FSM, VM, etc.)
- : BufferAccessStrategy for buffer management policy (unused for temporary relations)
- : uint32 containing operation flags controlling extension behavior
- : uint32 specifying the number of blocks to extend by
- : BlockNumber specifying the target block number to extend up to
- : Buffer array to receive handles for newly allocated blocks
- : Pointer to uint32 that receives the actual number of blocks extended

## Dependencies
- Functions called/Symbols referenced:
  - TRACE_POSTGRESQL_BUFFER_EXTEND_START
  - TRACE_POSTGRESQL_BUFFER_EXTEND_DONE
  - RELPERSISTENCE_TEMP
  - ExtendBufferedRelLocal
  - [ExtendBufferedRelShared](ExtendBufferedRelShared.md)
- Called from (representative examples):
  - [ExtendBufferedRelBy](ExtendBufferedRelBy.md)
  - [ExtendBufferedRelTo](ExtendBufferedRelTo.md)

## Notes and Other Information
- Routes extension operations based on relation persistence (temporary vs persistent)
- Provides consistent tracing for buffer extension operations across different relation types
- Central abstraction point that simplifies the interface for relation extension callers
- Maintains performance monitoring capabilities through DTrace-compatible tracing probes
- Part of PostgreSQL's unified buffer management architecture