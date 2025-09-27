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

## Simplified Source

```c
// Simplified version of WalSummarizerShmemInit
void WalSummarizerShmemInit(void) {
    bool found;

    // Create or attach to shared memory for WAL summarizer
    WalSummarizerCtl = (WalSummarizerData *)
        ShmemInitStruct("Wal Summarizer Ctl", WalSummarizerShmemSize(), &found);

    // If this is the first process, initialize with default values
    if (!found) {
        // Initialize control structure with placeholder values
        WalSummarizerCtl->initialized = false;
        WalSummarizerCtl->summarized_tli = 0;
        WalSummarizerCtl->summarized_lsn = InvalidXLogRecPtr;
        WalSummarizerCtl->lsn_is_exact = false;
        WalSummarizerCtl->summarizer_pgprocno = INVALID_PROC_NUMBER;
        WalSummarizerCtl->pending_lsn = InvalidXLogRecPtr;

        // Initialize condition variable for process coordination
        ConditionVariableInit(&WalSummarizerCtl->summary_file_cv);
    }
}
```

Key simplifications made:
- Consolidated the shared memory initialization logic flow
- Added clear comments explaining the two main phases: attach/create and initialize
- Preserved all essential initialization steps
- Maintained the original structure while improving readability