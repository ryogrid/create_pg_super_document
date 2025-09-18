# BufferGetPage

## Location
[src/include/storage/bufmgr.h:404-411](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/bufmgr.h#L404-L411)

## Overview
BufferGetPage is a static inline function that returns the page associated with a buffer in PostgreSQLs buffer management system.

## Definition
static inline Page BufferGetPage(Buffer buffer)

## Detailed Description
BufferGetPage is a convenience function that provides access to the page data associated with a buffer. It simply casts the result of BufferGetBlock() to a Page type, which is the standard PostgreSQL type for representing disk pages.

This function serves as a type-safe wrapper around BufferGetBlock(), ensuring that the returned pointer is treated as a Page rather than a generic Block. Since Page and Block are related types in PostgreSQLs type system, this cast provides the appropriate semantic meaning for code that needs to work with page-level operations.

The function inherits all the validation and behavior characteristics from BufferGetBlock(), including support for both local and shared buffers.

## Parameters / Member Variables
- buffer: Buffer identifier for which to retrieve the page pointer (type: Buffer)

## Dependencies
- Functions called/Symbols referenced:
  - [BufferGetBlock](BufferGetBlock.md) (function to get the block pointer)
  - Page (return type - PostgreSQL page type)
- Called from (representative examples):
  - Currently shows no direct references, but likely used throughout PostgreSQL codebase for page-level operations

## Notes and Other Information
- This is essentially a type-casting wrapper around BufferGetBlock()
- Provides semantic clarity by returning a Page type instead of generic Block
- Inherits all validation requirements from BufferGetBlock() (buffer must be valid)
- Used when code needs to perform page-level operations on buffer contents
- The Page type typically provides access to page header information and page structure
- Like BufferGetBlock(), this gives direct access to buffer memory that can be read or modified