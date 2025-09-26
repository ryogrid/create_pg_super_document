# tuplesort_markpos

## Location
src/backend/utils/sort/tuplesort.c: 2473 - 2503

## Overview
Saves the current read position in a completed tuple sort, creating a bookmark that can be returned to later using tuplesort_restorepos.

## Definition
```c
void tuplesort_markpos(Tuplesortstate *state)
```

## Detailed Description
The tuplesort_markpos function creates a position marker at the current location within a sorted result set, enabling subsequent restoration to this exact position. This functionality is crucial for operations that need to backtrack or re-read portions of sorted data, such as certain join algorithms or window functions. The function handles position marking differently based on storage location: for in-memory sorts (TSS_SORTEDINMEM), it saves the current tuple index; for tape-based sorts (TSS_SORTEDONTAPE), it uses LogicalTapeTell to capture the precise tape block and offset. The marked position includes the current EOF status, ensuring complete state preservation.

## Parameters / Member Variables
- `state`: Pointer to the Tuplesortstate structure representing the active tuple sort where the current position should be marked

## Dependencies
- Functions called/Symbols referenced:
  - Tuplesortstate (sorting state structure)
  - TUPLESORT_RANDOMACCESS (random access capability flag)
  - TSS_SORTEDINMEM (in-memory sorted status)
  - TSS_SORTEDONTAPE (tape-based sorted status)
  - LogicalTapeTell (retrieves current tape position)
- Called from (representative examples):
  - ExecSortMarkPos (executor mark position operation)

## Notes and Other Information
- Requires TUPLESORT_RANDOMACCESS option to be enabled during sort initialization
- Temporarily switches to sort context for memory management consistency
- For in-memory sorts: saves current tuple index and EOF flag to markpos_offset and markpos_eof
- For tape-based sorts: captures tape block/offset via LogicalTapeTell and saves EOF status
- Works in conjunction with tuplesort_restorepos for position restoration
- Throws error if called on sort in invalid state
- This is a public function exposed through tuplesort.h interface
- Essential for algorithms requiring backtracking or multiple position bookmarks in sorted data