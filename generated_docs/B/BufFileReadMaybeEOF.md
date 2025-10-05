# BufFileReadMaybeEOF

## Location
[src/backend/storage/file/buffile.c:664-675](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L664-L675)

## Overview
Reads exactly the specified number of bytes from a buffered file, with an option to allow end-of-file conditions without raising an error.

## Definition
```c
size_t BufFileReadMaybeEOF(BufFile *file, void *ptr, size_t size, bool eofOK)
```

## Detailed Description
BufFileReadMaybeEOF is a wrapper around BufFileReadCommon that provides controlled handling of end-of-file conditions. It attempts to read exactly the specified number of bytes from the buffered file, but unlike BufFileReadExact, it can optionally tolerate encountering EOF.

The function calls BufFileReadCommon with the 'exact' parameter set to true and the 'eofOK' parameter passed through from the caller. This means:
- It normally requires reading exactly 'size' bytes (no partial reads)
- If eofOK is true and zero bytes are read due to EOF, this is acceptable and no error is raised
- If eofOK is false, it behaves identically to BufFileReadExact

The function returns the actual number of bytes read, which will be either 'size' (successful full read) or 0 (EOF encountered when eofOK is true).

## Parameters / Member Variables
- `file`: Pointer to the BufFile structure representing the buffered file to read from
- `ptr`: Pointer to the buffer where the read data will be stored
- `size`: Number of bytes that should be read
- `eofOK`: Boolean flag indicating whether encountering EOF (and reading 0 bytes) is acceptable

## Dependencies
- Functions called/Symbols referenced:
  - [BufFileReadCommon](BufFileReadCommon.md) (internal function that performs the actual reading)
- Called from (representative examples):
  - [ExecHashJoinGetSavedTuple](../E/ExecHashJoinGetSavedTuple.md) (hash join execution, checking for saved tuples)
  - [apply_spooled_messages](../a/apply_spooled_messages.md) (logical replication, processing message batches)
  - [getlen](../g/getlen.md) (tuplestore operations, reading length prefixes)

## Notes and Other Information
- Returns the actual number of bytes read (either 'size' or 0)
- When eofOK is true and EOF is encountered immediately, returns 0 without error
- When eofOK is false, behaves identically to BufFileReadExact (raises error on any read failure)
- This function is commonly used when reading data structures that may or may not be present at the current file position
- Useful for protocols or file formats where EOF at specific points is a valid condition rather than an error
- The function automatically flushes any pending writes before attempting to read
- Partial reads (reading some but not all requested bytes) are still treated as errors regardless of the eofOK setting

## Simplified Source

```c
size_t BufFileReadMaybeEOF(BufFile *file, void *ptr, size_t size, bool eofOK) {
    // Read exactly the specified size, optionally allowing EOF
    // Returns size on success, 0 on EOF (if eofOK is true)
    return BufFileReadCommon(file, ptr, size, true, eofOK);
}
```