# decodePageSplitRecord

## Location
[src/backend/access/gist/gistxlog.c:223-246](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistxlog.c#L223-L246)

## Overview
Decodes a page split WAL record to extract an array of IndexTuple pointers from the serialized data.

## Definition
```c
static IndexTuple *decodePageSplitRecord(char *begin, int len, int *n)
```

## Detailed Description
This utility function parses serialized page split data from WAL records to reconstruct an array of IndexTuple pointers. The function processes binary data that was previously serialized during a GiST page split operation.

The serialization format is:
1. **Header**: First 4 bytes contain the number of tuples (int)
2. **Tuple Data**: Variable-length IndexTuple structures stored sequentially

The function performs the following steps:
1. Extracts the tuple count from the beginning of the data
2. Allocates memory for an array of IndexTuple pointers
3. Iterates through the serialized data, setting each pointer to the appropriate location
4. Advances the pointer by the size of each tuple using IndexTupleSize()
5. Validates that the entire data length was consumed correctly

The function includes assertions to ensure data integrity and proper parsing, checking both intermediate bounds and final length consumption.

## Parameters / Member Variables
- `begin`: Pointer to the start of the serialized page split data
- `len`: Length of the serialized data in bytes
- `n`: Output parameter that receives the number of tuples found

## Dependencies
- Functions called/Symbols referenced:
  - IndexTupleSize (calculates size of individual index tuples)
  - [palloc](../p/palloc.md) (allocates memory for tuple pointer array)
  - memcpy (copies tuple count from serialized data)
  - Assert (validates data parsing integrity)
- Called from (representative examples):
  - [gistRedoPageSplitRecord](../g/gistRedoPageSplitRecord.md) (processes page split WAL records)

## Notes and Other Information
- This is a static function only used within gistxlog.c
- Essential for WAL recovery of GiST page split operations  
- Does not allocate memory for the tuples themselves, only for the pointer array
- The returned IndexTuple pointers point directly into the provided data buffer
- Caller is responsible for the lifetime management of the input data buffer
- Uses assertions to validate proper parsing of the serialized format
- Part of the GiST index WAL recovery infrastructure
- Critical for reconstructing page split operations during database recovery