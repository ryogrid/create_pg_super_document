# CopyReadLine

## Location
src/backend/commands/copyfromparse.c: 1099 - 1174

## Overview
CopyReadLine reads the next complete input line from a COPY FROM operation and stores it in the line buffer, handling different end-of-line markers and EOF conditions.

## Definition


## Detailed Description
This static function is responsible for reading a complete line of input data during COPY FROM operations. It acts as a high-level wrapper around CopyReadLineText, providing additional processing for end-of-line handling and protocol-specific EOF behavior. The function first resets the line buffer and reads the raw line data, then processes the result based on whether EOF was encountered or a newline terminated the read. For non-EOF cases, it strips the appropriate end-of-line marker(s) based on the detected EOL type (NL, CR, or CRNL). For frontend protocol connections, it handles special EOF processing by consuming any remaining data after the EOF marker.

The function ensures that the final line buffer contains only the actual data without terminating characters, making it ready for subsequent parsing operations. It also manages the line_buf_valid flag to indicate when the buffer contains valid data for error reporting purposes.

## Parameters / Member Variables
- : The COPY FROM state structure containing the line buffer, EOL type information, and input source details

## Dependencies
- Functions called/Symbols referenced:
  - resetStringInfo: Clears and resets the line buffer string
  - CopyReadLineText: Performs the actual line reading from input source
  - CopyGetData: Reads additional data from frontend protocol connections
  - EOL_NL, EOL_CR, EOL_CRNL, EOL_UNKNOWN: End-of-line type constants
  - COPY_FRONTEND: Input source type constant
- Called from (representative examples):
  - NextCopyFromRawFields: Higher-level function that processes raw field data
  - NO_END_OF_COPY_GOTO: Error handling context

## Notes and Other Information
- Static function - only accessible within the copyfromparse.c module
- Returns true if the read was terminated by EOF, false if terminated by newline
- Automatically strips end-of-line markers from the final buffer content
- Handles different EOL conventions: Unix (\n), Mac (\r), and Windows (\r\n)
- Performs special protocol cleanup for frontend connections when EOF is encountered
- Sets line_buf_valid flag to true after successful processing for error reporting
- The line buffer is reset at the start of each call, discarding previous content
- Maintains buffer state information for subsequent parsing operations