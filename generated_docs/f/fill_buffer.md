# fill_buffer

## Location
[src/tools/pg_bsd_indent/io.c:346-467](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/io.c#L346-L467)

## Overview
Reads one line of input into the input buffer and sets up buffer pointers to manage the input stream for the PostgreSQL BSD indent tool.

## Definition

```c
void
fill_buffer(void)
```
## Detailed Description
The  function is a core input handling routine in the PostgreSQL BSD indent tool that manages reading input from files or standard input. It reads one line of input into the , setting up  and  to point to the line's start and end+1 respectively. The buffer does not get null-terminated.

The function handles several important scenarios:
- Restores partly filled input buffers from previous operations
- Dynamically grows the input buffer when lines exceed current capacity  
- Manages lookahead buffering for input parsing
- Handles EOF conditions by adding space and newline
- Processes special INDENT control comments (/**INDENT**, /*INDENT ON*/, /*INDENT OFF*/) that control formatting behavior
- When formatting is inhibited, directly copies input to output

The function includes logic to detect and handle indent control directives embedded in comments, allowing users to selectively enable/disable formatting for specific code sections.

## Parameters / Member Variables
This function takes no parameters but operates on several global variables:
- : Saved buffer pointer for partly filled input buffers
- : Saved buffer end pointer  
- : Current position in input buffer
- : End of current input buffer content
- : Main input buffer
- : Current size limit of input buffer
- , : Lookahead buffer management
- : Input file stream
- : EOF flag
- : Flag to disable formatting

## Dependencies
- Functions called/Symbols referenced:
  - : For dynamically growing input buffer
  - : For error handling when buffer allocation fails
  - : Recursive call to flush indent error messages
  - : To output current line when processing indent directives
- Called from (representative examples):
  - : Primary entry point calls (multiple locations)
  - : Lexical analyzer calls when more input needed
  - : Comment processing calls for additional input

## Notes and Other Information
- Originally coded in November 1976 by D A Willcox of CAC
- The function uses a sophisticated lookahead mechanism to handle complex parsing scenarios
- Buffer grows dynamically by doubling size plus 10 bytes when capacity is exceeded
- Special handling for INDENT control comments allows fine-grained control over code formatting
- When  is active, input is passed directly to output without processing
- The function is essential for the token-based parsing architecture of the indent tool