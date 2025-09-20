# bbstreamer_tar_parser

## Location
[src/bin/pg_basebackup/bbstreamer_tar.c:30-37](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_tar.c#L30-L37)

## Overview
A structure representing a TAR format parser that implements the bbstreamer interface to process TAR archive data streams in PostgreSQL's pg_basebackup utility.

## Definition

```c
typedef struct bbstreamer_tar_parser
{
	bbstreamer	base;
	bbstreamer_archive_context next_context;
	bbstreamer_member member;
	size_t		file_bytes_sent;
	size_t		pad_bytes_expected;
} bbstreamer_tar_parser;
```
## Detailed Description
The  structure is designed to parse TAR archive streams in pg_basebackup. It extends the base  structure to provide TAR-specific parsing functionality. This parser transforms a stream of unknown chunks into properly categorized chunks (header, content, trailer) following TAR format conventions. It maintains state information to track the current parsing context and progress through TAR archive members.

## Parameters / Member Variables
- : Base bbstreamer structure containing common streamer functionality (operations, next streamer, buffer)
- : Indicates the expected type of the next chunk to be processed (header, content, trailer, or archive trailer)
- : Contains metadata about the current archive member being processed (pathname, size, mode, ownership, etc.)
- : Tracks how many bytes of the current file's content have been processed
- : Number of padding bytes expected after the current file to align to TAR block boundaries

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer](bbstreamer.md) (base structure)
  - [bbstreamer_archive_context](bbstreamer_archive_context.md) (enumeration for chunk types)
  - bbstreamer_member (structure for member metadata)
- Called from (representative examples):
  - [bbstreamer_tar_parser_new](bbstreamer_tar_parser_new.md) (constructor function)
  - [bbstreamer_tar_parser_content](bbstreamer_tar_parser_content.md) (content processing function)
  - [bbstreamer_tar_header](bbstreamer_tar_header.md) (header processing function)
  - [bbstreamer_tar_parser_finalize](bbstreamer_tar_parser_finalize.md) (finalization function)

## Notes and Other Information
- This structure is specifically used in src/bin/pg_basebackup/bbstreamer_tar.c:30-37
- The parser follows TAR format specifications, handling proper block alignment and padding
- It's designed to work as part of a streaming pipeline where data can be processed incrementally
- The structure maintains state between calls to handle partial reads and TAR block boundaries correctly