# _add

## Location
src/timezone/strftime.c: 525 - 540

## Overview
Low-level utility function that safely copies a null-terminated string to the output buffer with bounds checking to prevent buffer overflows.

## Definition


## Detailed Description
_add is a fundamental building block in PostgreSQL's strftime implementation that handles the safe copying of strings into the output buffer. It performs bounds-checked string copying, ensuring that the destination buffer is never overrun. The function copies characters from the source string to the destination buffer one by one, stopping when it encounters a null terminator or reaches the buffer limit.

The function is designed to be safe and efficient, providing a controlled way to append strings to the formatted output buffer. It maintains the current write position in the buffer and ensures that buffer boundaries are never exceeded, which is crucial for preventing security vulnerabilities and memory corruption.

Unlike standard string functions like strcpy or strcat, _add incorporates explicit bounds checking with the ptlim parameter, making it suitable for use in security-sensitive contexts where buffer overflows must be prevented.

## Parameters / Member Variables
- : Source null-terminated string to be copied to the output buffer
- : Current write position in the destination buffer
- : Pointer to the end of the destination buffer (exclusive limit for bounds checking)

## Dependencies
- Functions called/Symbols referenced:
  - No external function calls (uses only basic C operations)
- Called from (representative examples):
  - _fmt (for adding literal strings, weekday names, month names)
  - _fmt (for adding AM/PM indicators, timezone names)
  - _fmt (for adding newlines, tabs, and other special characters)
  - _conv (for adding formatted numeric strings)

## Notes and Other Information
- Provides bounds-safe string copying with explicit buffer limit checking
- Returns updated write position in the buffer for chaining operations
- Stops copying if buffer limit is reached, preventing buffer overflow
- Does not null-terminate the destination buffer (caller responsible for final null termination)
- Essential for secure string handling in formatted output generation
- Simple but critical function used extensively throughout the strftime formatting process
- The function design allows for efficient chaining of multiple string append operations