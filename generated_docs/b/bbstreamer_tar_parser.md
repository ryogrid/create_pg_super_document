# bbstreamer_tar_parser

## Location
src/bin/pg_basebackup/bbstreamer_tar.c: 30 - 37

## Overview
A structure representing a TAR format parser that implements the bbstreamer interface to process TAR archive data streams in PostgreSQL's pg_basebackup utility.

## Definition


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
  - bbstreamer (base structure)
  - bbstreamer_archive_context (enumeration for chunk types)
  - bbstreamer_member (structure for member metadata)
- Called from (representative examples):
  - bbstreamer_tar_parser_new (constructor function)
  - bbstreamer_tar_parser_content (content processing function)
  - bbstreamer_tar_header (header processing function)
  - bbstreamer_tar_parser_finalize (finalization function)

## Notes and Other Information
- This structure is specifically used in src/bin/pg_basebackup/bbstreamer_tar.c:30-37
- The parser follows TAR format specifications, handling proper block alignment and padding
- It's designed to work as part of a streaming pipeline where data can be processed incrementally
- The structure maintains state between calls to handle partial reads and TAR block boundaries correctly