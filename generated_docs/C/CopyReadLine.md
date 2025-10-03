# CopyReadLine

## Location
[src/backend/commands/copyfromparse.c:1099-1174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copyfromparse.c#L1099-L1174)

## Overview
CopyReadLine reads the next complete input line from a COPY FROM operation and stores it in the line buffer, handling different end-of-line markers and EOF conditions.

## Definition

```c
static bool
CopyReadLine(CopyFromState cstate)
```
## Detailed Description
This static function is responsible for reading a complete line of input data during COPY FROM operations. It acts as a high-level wrapper around CopyReadLineText, providing additional processing for end-of-line handling and protocol-specific EOF behavior. The function first resets the line buffer and reads the raw line data, then processes the result based on whether EOF was encountered or a newline terminated the read. For non-EOF cases, it strips the appropriate end-of-line marker(s) based on the detected EOL type (NL, CR, or CRNL). For frontend protocol connections, it handles special EOF processing by consuming any remaining data after the EOF marker.

The function ensures that the final line buffer contains only the actual data without terminating characters, making it ready for subsequent parsing operations. It also manages the line_buf_valid flag to indicate when the buffer contains valid data for error reporting purposes.

## Parameters / Member Variables
- `cstate`: The COPY FROM state structure containing the line buffer, EOL type information, and input source details
## Dependencies
- Functions called/Symbols referenced:
  - [resetStringInfo](../r/resetStringInfo.md): Clears and resets the line buffer string
  - [CopyReadLineText](CopyReadLineText.md): Performs the actual line reading from input source
  - [CopyGetData](CopyGetData.md): Reads additional data from frontend protocol connections
  - EOL_NL, EOL_CR, EOL_CRNL, EOL_UNKNOWN: End-of-line type constants
  - COPY_FRONTEND: Input source type constant
- Called from (representative examples):
  - [NextCopyFromRawFields](../N/NextCopyFromRawFields.md): Higher-level function that processes raw field data
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

## Simplified Source

```c
static bool
CopyReadLine(CopyFromState cstate)
{
    bool result;

    // Reset line buffer for new input
    resetStringInfo(&cstate->line_buf);
    cstate->line_buf_valid = false;

    // Read the actual line data
    result = CopyReadLineText(cstate);

    if (result) {
        // EOF reached: handle protocol-specific cleanup
        if (cstate->copy_src == COPY_FRONTEND) {
            // For frontend connections, consume any remaining data after EOF marker
            int inbytes;
            do {
                inbytes = CopyGetData(cstate, cstate->input_buf, 1, INPUT_BUF_SIZE);
            } while (inbytes > 0);

            // Reset buffer positions
            cstate->input_buf_index = 0;
            cstate->input_buf_len = 0;
            cstate->raw_buf_index = 0;
            cstate->raw_buf_len = 0;
        }
    } else {
        // Line terminated by newline: strip the EOL marker(s)
        switch (cstate->eol_type) {
            case EOL_NL:  // Unix: \n
                cstate->line_buf.len--;
                cstate->line_buf.data[cstate->line_buf.len] = '\0';
                break;
            case EOL_CR:  // Mac: \r
                cstate->line_buf.len--;
                cstate->line_buf.data[cstate->line_buf.len] = '\0';
                break;
            case EOL_CRNL:  // Windows: \r\n
                cstate->line_buf.len -= 2;
                cstate->line_buf.data[cstate->line_buf.len] = '\0';
                break;
            case EOL_UNKNOWN:
                // Should not happen
                Assert(false);
                break;
        }
    }

    // Mark buffer as valid for error reporting
    cstate->line_buf_valid = true;

    return result;  // true = EOF, false = newline
}
```