# WalSummarizerData

## Location
[src/backend/postmaster/walsummarizer.c:93-103](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/walsummarizer.c#L93-L103)

## Overview
WalSummarizerData is a shared memory structure that maintains the state and coordination information for the PostgreSQL WAL (Write-Ahead Log) summarizer process.

## Definition


## Detailed Description
WalSummarizerData serves as the central coordination structure for WAL summarization in PostgreSQL's shared memory. It tracks the progress of WAL summarization, manages the state of the summarizer process, and provides synchronization mechanisms for processes that depend on WAL summary files. The structure is protected by WALSummarizerLock (except for the condition variable which handles its own synchronization) and is essential for incremental backup functionality.

## Parameters / Member Variables
- : Boolean flag indicating whether the summarizer has discovered existing summary files and populated the shared memory state
- : Timeline ID indicating the last timeline for which summarization has been completed
- : LSN (Log Sequence Number) indicating the last position that has been summarized
- : Boolean flag indicating whether summarized_lsn is exact (true) or an approximation (false)
- : Process number of the running summarizer process, or INVALID_PROC_NUMBER if none is running
- : LSN advertised by the summarizer indicating the ending position of recently read records (may be ahead of summarized_lsn due to buffering)
- : Condition variable for synchronization, handles its own locking mechanism

## Dependencies
- Functions called/Symbols referenced: None directly (this is a data structure)
- Referenced by:
  - [WalSummarizerShmemSize](WalSummarizerShmemSize.md) (for calculating shared memory requirements)
  - [WalSummarizerShmemInit](WalSummarizerShmemInit.md) (for initializing the shared memory structure)
  - Various functions throughout walsummarizer.c for state management

## Notes and Other Information
- Most fields are protected by WALSummarizerLock except for summary_file_cv which provides its own synchronization
- The lsn_is_exact field handles cases where the starting LSN must be approximated (e.g., when no existing summary files are found at startup)
- The pending_lsn field allows the summarizer to advertise progress before actually writing summary files, supporting buffered operations
- This structure is crucial for incremental backup coordination, allowing other processes to wait for summarization to reach specific LSNs