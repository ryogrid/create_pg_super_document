# LogicalTapeImport

## Location
[src/backend/utils/sort/logtape.c:609-666](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/logtape.c#L609-L666)

## Overview
Claims ownership of a logical tape from a worker process's shared BufFile and imports it into the leader's tape set, creating a concatenated view of multiple BufFiles for parallel sort operations.

## Definition
```c
LogicalTape *LogicalTapeImport(LogicalTapeSet *lts, int worker, TapeShare *shared)
```

## Detailed Description
The `LogicalTapeImport` function is used in parallel sorting operations by the leader process to import logical tapes that were created by worker processes. The function opens the worker's BufFile from the shared fileset, calculates the appropriate block offsets, and either establishes it as the primary BufFile (if it's the first import) or appends it to the existing concatenated BufFile structure.

The function creates a unified view of multiple worker BufFiles by concatenating them together, while maintaining proper block offset tracking. Each imported tape remembers its starting block number within the concatenated file structure. The leader process treats imported tapes as unfrozen (unlike in worker processes), which allows for larger read buffers optimized for sequential access rather than the smaller buffers used for random access in frozen tapes.

Block accounting is carefully maintained to track allocated blocks, written blocks, and hole blocks (gaps between concatenated files) for accurate instrumentation and space management.

## Parameters / Member Variables
- `lts`: Pointer to the LogicalTapeSet into which the tape should be imported
- `worker`: Worker process identifier used to construct the BufFile filename
- `shared`: Pointer to TapeShare structure containing shared information about the tape, including the first block number

## Dependencies
- Functions called/Symbols referenced:
  - [ltsCreateTape](../l/ltsCreateTape.md) (creates new LogicalTape structure)
  - [pg_itoa](../p/pg_itoa.md) (converts worker ID to filename string)
  - [BufFileOpenFileSet](../B/BufFileOpenFileSet.md) (opens worker's BufFile from shared fileset)
  - [BufFileSize](../B/BufFileSize.md) (gets size of worker's BufFile)
  - [BufFileAppend](../B/BufFileAppend.md) (appends worker BufFile to leader's concatenated file)
  - Min (minimum value macro)
  - MaxAllocSize (maximum allocation size constant)
  - [TapeShare](../T/TapeShare.md) (shared tape information structure)
- Called from (representative examples):
  - [leader_takeover_tapes](../l/leader_takeover_tapes.md) (parallel tuplesort leader takeover)

## Notes and Other Information
- This function should only be called by the leader process, not by workers
- The first imported tape becomes the base BufFile (`lts->pfile`), while subsequent tapes are appended to it
- Imported tapes are not frozen in the leader, allowing for larger read buffers compared to worker processes
- The `max_size` for the read buffer is limited to the smaller of MaxAllocSize and the actual file size
- Block accounting includes hole blocks that represent gaps between concatenated BufFiles
- The function assumes the shared fileset uses worker numbers as filenames (converted via `pg_itoa`)
- Proper offset tracking ensures that each tape knows its position within the concatenated file structure
- The concatenated file approach allows the leader to have a unified view of all worker tape data while maintaining logical separation