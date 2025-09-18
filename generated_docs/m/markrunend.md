# markrunend

## Location
[src/backend/utils/sort/tuplesort.c:2907-2920](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplesort.c#L2907-L2920)

## Overview
Writes a zero-length marker to a logical tape to indicate the end of a run during tuple sorting operations.

## Definition


## Detailed Description
The `markrunend` function writes a special end-of-run marker to a logical tape by writing a zero-length value (4 bytes containing 0). This marker serves as a delimiter between different runs of sorted data on the tape during merge sort operations. When reading from tapes, the zero-length marker signals to the reading functions that they have reached the end of the current run and should proceed to the next run or stop processing.

This function is essential for the multi-way merge algorithm used in PostgreSQL's external sorting, where multiple sorted runs are merged together to produce the final sorted output.

## Parameters / Member Variables
- `tape`: Pointer to the LogicalTape where the end-of-run marker should be written

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalTape](../L/LogicalTape.md) (structure type)
  - LogicalTapeWrite (function for writing to logical tapes)
- Called from (representative examples):
  - mergeonerun (src/backend/utils/sort/tuplesort.c:2283)
  - [dumptuples](../d/dumptuples.md) (src/backend/utils/sort/tuplesort.c:2426)
  - LEADER macro usage (src/backend/utils/sort/tuplesort.c:476)

## Notes and Other Information
- This is a static function, only accessible within the tuplesort.c compilation unit
- The zero-length marker is compatible with the `getlen` function, which can handle zero-length values when the `eofOK` parameter is true
- Used during the "dump" phase of external sorting when writing sorted runs to temporary storage
- Essential for proper delimitation of runs during multi-way merge operations
- The marker consists of exactly 4 bytes (sizeof(unsigned int)) containing the value 0