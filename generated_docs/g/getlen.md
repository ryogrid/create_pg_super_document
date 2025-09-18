# getlen

## Location
[src/backend/utils/sort/tuplestore.c:1466-1489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/tuplestore.c#L1466-L1489)

## Overview
A low-level tape interface routine that reads the length prefix from a logical tape, used in tuple sorting operations to determine the size of the next data item to be read.

## Definition
```c
static unsigned int getlen(LogicalTape *tape, bool eofOK)
```

## Detailed Description
This function is part of the tape interface routines used in PostgreSQL sorting operations. It reads a length value (unsigned int) from the beginning of the next record on a logical tape. The length value serves as a prefix that indicates how many bytes the following data item contains. The function includes error handling for unexpected end-of-tape conditions and can optionally allow end-of-file conditions based on the eofOK parameter.

## Parameters / Member Variables
- `tape`: The logical tape to read the length prefix from
- `eofOK`: Boolean flag indicating whether end-of-file condition is acceptable (true) or should trigger an error (false)

## Dependencies
- Functions called/Symbols referenced:
  - [LogicalTape](../L/LogicalTape.md)
  - LogicalTapeRead
- Called from (representative examples):
  - [tuplesort_gettuple_common](../t/tuplesort_gettuple_common.md)
  - mergereadnext
  - tuplestore_gettuple

## Notes and Other Information
- This is a static function, only accessible within the tuplesort.c module
- Part of the tape interface system used for external sorting when data exceeds memory capacity
- Returns 0 when end-of-data is reached (if eofOK is true)
- Throws ERROR if unexpected end of tape occurs during the read operation
- Throws ERROR if end-of-data (len == 0) is encountered when eofOK is false
- The length value read is used by callers to determine how much additional data to read from the tape