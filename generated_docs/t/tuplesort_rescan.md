# tuplesort_rescan

## Location
[src/backend/utils/sort/tuplesort.c:2440-2472](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L2440-L2472)

## Overview
Rewinds a completed tuple sort to the beginning, allowing the sorted results to be read again from the start, supporting operations that require multiple passes over sorted data.

## Definition
```c
void tuplesort_rescan(Tuplesortstate *state)
```

## Detailed Description
The tuplesort_rescan function enables rescanning of a completed tuple sort by resetting the read position to the beginning and clearing end-of-file flags. This function is essential for operations that need to traverse the sorted results multiple times, such as certain aggregate functions or subquery evaluations. The function handles two different storage scenarios: when results are still in memory (TSS_SORTEDINMEM), it simply resets the current tuple index; when results are stored on tape (TSS_SORTEDONTAPE), it rewinds the logical tape and resets tape position markers. The function requires that the sort was configured with TUPLESORT_RANDOMACCESS option to enable this functionality.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate structure representing the completed tuple sort operation that needs to be rescanned

## Dependencies
- Functions called/Symbols referenced:
  - Tuplesortstate (sorting state structure)
  - TUPLESORT_RANDOMACCESS (random access option flag)
  - TSS_SORTEDINMEM (sorted in memory status)
  - TSS_SORTEDONTAPE (sorted on tape status)
  - LogicalTapeRewindForRead (rewinds tape for reading)
- Called from (representative examples):
  - ExecReScanSort (executor rescan operation)
  - percentile_disc_final (percentile aggregate function)
  - percentile_cont_final_common (continuous percentile function)
  - mode_final (mode aggregate function)

## Notes and Other Information
- Requires TUPLESORT_RANDOMACCESS option to be set during sort initialization
- Switches to sort context temporarily for memory management
- Resets different state variables based on storage location (memory vs tape)
- For in-memory sorts: resets current tuple index and EOF flags
- For tape-based sorts: rewinds tape and resets position markers
- Throws error if called on sort in invalid state
- This is a public function exposed through tuplesort.h interface
- Essential for aggregate functions and operations requiring multiple result set traversals