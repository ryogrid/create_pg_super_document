# BufFileRead

## Location
[src/backend/storage/file/buffile.c:645-653](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/buffile.c#L645-L653)

## Overview
BufFileRead provides a legacy interface for reading data from BufFiles, allowing partial reads and requiring caller handling of EOF conditions.

## Definition

```c
size_t
BufFileRead(BufFile *file, void *ptr, size_t size)
```
## Detailed Description
BufFileRead is a public API function that provides the standard, legacy interface for reading data from BufFiles. It serves as a thin wrapper around BufFileReadCommon, configured for maximum flexibility by allowing short reads and not enforcing exact byte counts.

This function is designed for callers who need to handle end-of-file conditions and partial reads themselves, similar to the behavior of the standard library's fread() function. It will read up to the requested number of bytes but may return fewer bytes if end-of-file is reached or if the underlying file system cannot provide the full requested amount in a single operation.

The function is part of PostgreSQL's legacy BufFile interface, maintained for compatibility with existing code that expects traditional file-like read semantics where the caller must check the return value to determine how many bytes were actually read.

## Parameters / Member Variables
- `*file`: Pointer to the BufFile structure to read from
- `*ptr`: Destination buffer where read data will be stored
- `size`: Maximum number of bytes to read
## Dependencies
- Functions called/Symbols referenced:
  - [BufFileReadCommon](BufFileReadCommon.md) (the underlying implementation with exact=false, eofOK=false)
- Called from (representative examples):
  - Currently no direct callers found in the codebase (legacy interface)

## Notes and Other Information
- This is a legacy interface maintained for backward compatibility
- Returns the actual number of bytes read, which may be less than requested
- Does not enforce exact read sizes - callers must check the return value
- Does not treat EOF as an error condition - callers must handle short reads
- Provides the most flexible read behavior among the BufFile read functions
- Part of the public BufFile API exposed in buffile.h
- Modern code typically uses BufFileReadExact for stricter read requirements
- The function parameters mirror those of standard fread() for familiar behavior

## Simplified Source

```c
size_t BufFileRead(BufFile *file, void *ptr, size_t size) {
    // Legacy wrapper for reading from BufFile with flexible EOF handling
    // Allows partial reads - caller must check return value
    return BufFileReadCommon(file, ptr, size, false, false);
}
```