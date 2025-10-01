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

## Simplified Source

```c
static bool
CopyReadLineText(CopyFromState cstate)
{
    char *copy_input_buf;
    int input_buf_ptr;
    int copy_buf_len;
    bool need_data = false;
    bool hit_eof = false;
    bool result = false;

    // CSV state variables
    bool first_char_in_line = true;
    bool in_quote = false, last_was_esc = false;
    char quotec = '\0', escapec = '\0';

    // Initialize CSV mode settings
    if (cstate->opts.csv_mode) {
        quotec = cstate->opts.quote[0];
        escapec = cstate->opts.escape[0];
        if (quotec == escapec) escapec = '\0';  // Ignore if same as quote
    }

    // Optimize buffer access with local variables
    copy_input_buf = cstate->input_buf;
    input_buf_ptr = cstate->input_buf_index;
    copy_buf_len = cstate->input_buf_len;

    for (;;) {
        char c;

        // Load more data if needed
        if (input_buf_ptr >= copy_buf_len || need_data) {
            REFILL_LINEBUF;
            CopyLoadInputBuf(cstate);
            // Update local variables after buffer reload
            hit_eof = cstate->input_reached_eof;
            input_buf_ptr = cstate->input_buf_index;
            copy_buf_len = cstate->input_buf_len;

            if (INPUT_BUF_BYTES(cstate) <= 0) {
                result = true;  // EOF reached
                break;
            }
            need_data = false;
        }

        // Get next character
        int prev_raw_ptr = input_buf_ptr;
        c = copy_input_buf[input_buf_ptr++];

        // Handle CSV quote/escape logic
        if (cstate->opts.csv_mode) {
            // Force lookahead for special characters
            if (c == '\\' || c == '\r') {
                IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
            }

            // Update CSV state
            if (in_quote && c == escapec) last_was_esc = !last_was_esc;
            if (c == quotec && !last_was_esc) in_quote = !in_quote;
            if (c != escapec) last_was_esc = false;

            // Track line numbers for embedded newlines in quotes
            if (in_quote && c == (cstate->eol_type == EOL_NL ? '\n' : '\r'))
                cstate->cur_lineno++;
        }

        // Handle carriage return (\r)
        if (c == '\r' && (!cstate->opts.csv_mode || !in_quote)) {
            // Detect and handle \r\n vs \r line endings
            if (cstate->eol_type == EOL_UNKNOWN || cstate->eol_type == EOL_CRNL) {
                IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
                c = copy_input_buf[input_buf_ptr];
                if (c == '\n') {
                    input_buf_ptr++;
                    cstate->eol_type = EOL_CRNL;
                } else {
                    cstate->eol_type = EOL_CR;
                }
            }
            break;  // Line terminator found
        }

        // Handle newline (\n)
        if (c == '\n' && (!cstate->opts.csv_mode || !in_quote)) {
            cstate->eol_type = EOL_NL;
            break;  // Line terminator found
        }

        // Handle end-of-copy marker (\.)
        if (c == '\\' && (!cstate->opts.csv_mode || first_char_in_line)) {
            IF_NEED_REFILL_AND_NOT_EOF_CONTINUE(0);
            char c2 = copy_input_buf[input_buf_ptr];

            if (c2 == '.') {
                input_buf_ptr++;  // Consume the '.'

                // Validate end-of-copy marker format
                // (Detailed validation logic for different EOL types)

                // Transfer data before \. to line buffer
                if (prev_raw_ptr > cstate->input_buf_index) {
                    appendBinaryStringInfo(&cstate->line_buf,
                                         cstate->input_buf + cstate->input_buf_index,
                                         prev_raw_ptr - cstate->input_buf_index);
                }
                cstate->input_buf_index = input_buf_ptr;
                result = true;  // EOF marker found
                break;
            } else if (!cstate->opts.csv_mode) {
                input_buf_ptr++;  // Skip character after backslash
            }
        }

        first_char_in_line = false;
    }

    // Transfer any remaining data to line buffer
    REFILL_LINEBUF;
    return result;
}
```