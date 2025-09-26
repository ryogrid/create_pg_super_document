# BlockRefTable

## Location
[src/common/blkreftable.c:144-154](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/blkreftable.c#L144-L154)

## Overview
BlockRefTable is the main container structure that encapsulates a hash table for tracking block reference information across relation forks, along with memory management context.

## Definition
```c
struct BlockRefTable
{
    blockreftable_hash *hash;
#ifndef FRONTEND
    MemoryContext mcxt;
#endif
};
```

## Detailed Description
BlockRefTable serves as the primary interface structure for PostgreSQL's block reference tracking system. It wraps a specialized hash table (blockreftable_hash) that stores BlockRefTableEntry structures, providing efficient lookup and management of block modification status across multiple relation forks. The structure abstracts the underlying hash table implementation from external callers, providing a clean API boundary. For backend processes, it also maintains an explicit memory context to ensure all allocations are grouped together for efficient memory management.

## Parameters / Member Variables
- `hash`: Pointer to the underlying hash table (blockreftable_hash) that stores the actual block reference entries
- `mcxt`: MemoryContext for managing all allocations related to this block reference table (only available in backend, not frontend code)

## Dependencies
- Functions called/Symbols referenced:
  - FRONTEND (preprocessor conditional compilation check)
- Used by:
  - [IncrementalBackupInfo](../I/IncrementalBackupInfo.md) (incremental backup system)
  - Various WAL summarizer functions (SummarizeWAL, SummarizeDbaseRecord, SummarizeSmgrRecord, SummarizeXactRecord)
  - [BlockRefTableWriter](BlockRefTableWriter.md) structure
  - [CreateEmptyBlockRefTable](../C/CreateEmptyBlockRefTable.md)
  - [BlockRefTableSetLimitBlock](BlockRefTableSetLimitBlock.md)
  - [BlockRefTableMarkBlockModified](BlockRefTableMarkBlockModified.md)
  - [BlockRefTableGetEntry](BlockRefTableGetEntry.md)
  - [WriteBlockRefTable](../W/WriteBlockRefTable.md)
  - Various header function declarations

## Notes and Other Information
- Defined in src/common/blkreftable.c:144-154 with documentation at lines 137-150
- Acts as an abstraction layer over the internal hash table implementation
- Memory context is conditionally compiled out for frontend tools to reduce dependencies
- Essential component of PostgreSQL's incremental backup and WAL summarization functionality
- The hash table uses BlockRefTableKey as keys and BlockRefTableEntry as values
- Provides memory management benefits by grouping all related allocations in a single context