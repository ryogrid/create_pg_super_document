# bbstreamer_tar_parser_content

## Location
[src/bin/pg_basebackup/bbstreamer_tar.c:111-260](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_tar.c#L111-L260)

## Overview
Parses unknown content as tar data by implementing a state machine that processes tar headers, contents, and trailers sequentially.

## Definition
```c
static void bbstreamer_tar_parser_content(bbstreamer *streamer, bbstreamer_member *member,
                                         const char *data, int len,
                                         bbstreamer_archive_context context)
```

## Detailed Description
This function implements the core parsing logic for the tar parser, operating as a state machine that handles different phases of tar file processing. It expects unparsed input (BBSTREAMER_UNKNOWN context) and processes it through four distinct states: BBSTREAMER_MEMBER_HEADER (parsing tar headers), BBSTREAMER_MEMBER_CONTENTS (streaming file contents), BBSTREAMER_MEMBER_TRAILER (handling padding), and BBSTREAMER_ARCHIVE_TRAILER (processing end-of-archive markers). The parser maintains internal state to track the current parsing position and expected data types.

## Parameters / Member Variables
- `streamer`: The bbstreamer instance containing parser state and operations
- `member`: Expected to be NULL for unparsed input
- `data`: Raw input data to be parsed as tar content
- `len`: Length of the input data buffer
- `context`: Expected to be BBSTREAMER_UNKNOWN for this parser

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer_buffer_until](bbstreamer_buffer_until.md)
  - [bbstreamer_tar_header](bbstreamer_tar_header.md)
  - [bbstreamer_content](bbstreamer_content.md)
  - [bbstreamer_buffer_bytes](bbstreamer_buffer_bytes.md)
  - TAR_BLOCK_SIZE
  - [pg_fatal](../p/pg_fatal.md)
- Called from (representative examples):
  - No direct references found (likely called through function pointer in ops structure)

## Notes and Other Information
- Implements a state machine with four primary states for different tar parsing phases
- Uses assertions to validate input expectations (member should be NULL, context should be BBSTREAMER_UNKNOWN)
- Handles tar block alignment requirements (TAR_BLOCK_SIZE = 512 bytes)
- Manages padding bytes calculation and trailer processing for proper tar format compliance
- Includes error checking for malformed archives (trailer exceeding 2 blocks)
- Maintains file size tracking to ensure complete content transmission
- Resets internal buffer state appropriately when transitioning between parsing phases