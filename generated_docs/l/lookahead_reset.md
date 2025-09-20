# lookahead_reset

## Location
[src/tools/pg_bsd_indent/io.c:320-345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tools/pg_bsd_indent/io.c#L320-L345)

## Overview
Resets the lookahead mechanism to begin scanning from just beyond the current input buffer position.

## Definition

```c
void
lookahead_reset(void)
```
## Detailed Description
The lookahead_reset function reinitializes the lookahead system to start scanning from the position immediately following the current main input buffer. This is essential for maintaining proper synchronization between the main parsing buffer and the lookahead buffer, especially when the parser needs to restart lookahead operations from a known position.

The function resets two key components: it repositions the main lookahead buffer pointer to the start of the lookahead region, and it sets up the saved buffer pointer to ensure that any previously saved buffer content is processed first during subsequent lookahead operations.

## Parameters / Member Variables
This function takes no parameters but modifies several global variables:
- : Reset to  to begin scanning from the start of the lookahead buffer
- : Set to  to ensure saved buffer content is processed first
- : The beginning position of the valid lookahead data
- : Pointer to saved buffer content that should be processed before new lookahead data

## Dependencies
- Functions called/Symbols referenced:
  - (No function calls - only manipulates global variables)
- Called from (representative examples):
  - [is_func_definition](../i/is_func_definition.md) (in lexi.c to restart function definition parsing)

## Notes and Other Information
- Essential companion function to lookahead() for proper lookahead buffer management
- Must be called before beginning a new lookahead scanning sequence
- Ensures that saved buffer content is properly integrated into the lookahead process
- Critical for maintaining parsing state consistency in pg_bsd_indent
- Commonly used in parsing contexts where multiple lookahead attempts may be needed
- Simple but crucial for preventing buffer synchronization issues between main parsing and lookahead operations