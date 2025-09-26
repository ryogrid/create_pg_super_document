# CopyReadLineText

## Location
[src/backend/commands/copyfromparse.c:1175-1508](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfromparse.c#L1175-L1508)

## Overview
CopyReadLineText is the core line-reading engine that handles byte-by-byte parsing of input data for COPY FROM operations, managing CSV quoting, escape sequences, end-of-line detection, and end-of-copy markers.

## Definition

```c
static bool
CopyReadLineText(CopyFromState cstate)
```
## Detailed Description
This static function performs the low-level parsing of text input during COPY FROM operations. It operates as the inner loop of CopyReadLine, handling the complex logic of reading input data byte-by-byte while respecting CSV quoting rules, detecting various end-of-line conventions, and recognizing the end-of-copy marker (\.). The function maintains state for CSV mode including quote tracking, escape sequence handling, and proper treatment of embedded newlines within quoted fields.

The function uses an optimized approach by processing input in chunks when possible, moving data from the input buffer to the line buffer efficiently. It automatically detects and handles different end-of-line conventions (Unix \n, Mac \r, Windows \r\n) and maintains consistency throughout the input. For CSV mode, it carefully tracks whether characters appear within quoted fields to determine their special meaning. The function also handles the PostgreSQL-specific end-of-copy marker (\.) while respecting CSV quoting rules.

## Parameters / Member Variables
- : The COPY FROM state structure containing input buffers, CSV options, EOL type tracking, and parsing state information

## Dependencies
- Functions called/Symbols referenced:
  - [CopyLoadInputBuf](CopyLoadInputBuf.md): Loads more data into the input buffer when needed
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md): Efficiently appends binary data to the line buffer
  - REFILL_LINEBUF: Macro that transfers pending data from input buffer to line buffer
  - IF_NEED_REFILL_AND_NOT_EOF_CONTINUE: Macro for conditional data loading and loop continuation
  - IF_NEED_REFILL_AND_EOF_BREAK: Macro for conditional data loading with EOF handling
  - NO_END_OF_COPY_GOTO: Macro for handling invalid end-of-copy sequences in CSV mode
  - EOL_NL, EOL_CR, EOL_CRNL, EOL_UNKNOWN: End-of-line type constants
- Called from (representative examples):
  - [CopyReadLine](CopyReadLine.md): Higher-level line reading wrapper that handles EOL stripping
  - NO_END_OF_COPY_GOTO: Error recovery mechanism for invalid end-of-copy sequences

## Notes and Other Information
- Static function - only accessible within the copyfromparse.c module
- Returns true if EOF was encountered, false if line was terminated by newline
- Handles all major end-of-line conventions and automatically detects the format on first use
- Maintains CSV state including quote tracking and escape sequence processing
- Processes input in chunks for efficiency while maintaining byte-level accuracy for special characters
- Recognizes \. as end-of-copy marker with strict validation of following characters
- In CSV mode, \. is only recognized when it appears at the start of a line (first_char_in_line)
- Handles embedded newlines within CSV quoted fields correctly
- Uses local variables for input buffer access to optimize the tight parsing loop
- Maintains line number counting for embedded newlines in CSV quoted fields
- Supports proper escape sequence handling where escape and quote characters may be the same or different