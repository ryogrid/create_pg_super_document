# WalSummarizerShmemInit

## Location
[src/backend/postmaster/walsummarizer.c:180-210](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/walsummarizer.c#L180-L210)

## Overview
Initializes or attaches to the shared memory segment used by the WAL summarizer module.

## Definition

```c
void
WalSummarizerShmemInit(void)
```
## Detailed Description
This function creates or attaches to the shared memory structure for WAL summarization. It uses ShmemInitStruct to either allocate new shared memory (if this is the first process) or attach to existing shared memory (if already created by another process). When creating new shared memory, it initializes the WalSummarizerData structure with default values. The actual meaningful initialization happens later when GetOldestUnsummarizedLSN() is called for the first time.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [WalSummarizerData](WalSummarizerData.md) (structure type being initialized)
  - [WalSummarizerShmemSize](WalSummarizerShmemSize.md) (to get required memory size)
  - [ShmemInitStruct](../S/ShmemInitStruct.md) (PostgreSQL shared memory allocation)
  - INVALID_PROC_NUMBER (constant for invalid process number)
  - [ConditionVariableInit](../C/ConditionVariableInit.md) (initializes condition variable)
- Called from (representative examples):
  - [CreateOrAttachShmemStructs](../C/CreateOrAttachShmemStructs.md) (in src/backend/storage/ipc/ipci.c:346)

## Notes and Other Information
- Sets WalSummarizerCtl global pointer to the shared memory structure
- Initial values are placeholders - real initialization occurs during first GetOldestUnsummarizedLSN() call
- Initializes condition variable for inter-process coordination
- Part of PostgreSQL's shared memory subsystem initialization
- Location: src/backend/postmaster/walsummarizer.c:180-210