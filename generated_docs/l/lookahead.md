# lookahead

## Location
src/tools/pg_bsd_indent/io.c: 275 - 319

## Overview
Provides look-ahead capability for reading input characters beyond the current buffer position without consuming them permanently.

## Definition


## Detailed Description
The lookahead function enables reading characters from the input stream ahead of the current position in the main input buffer. It maintains a separate lookahead buffer that can be read multiple times and reset as needed. This is essential for parsing decisions that require examining upcoming tokens without committing to consuming them.

The function first checks if there are saved characters from a previous buffer state (bp_save area), then manages a dynamically-sized lookahead buffer. When the lookahead buffer is exhausted, it reads additional characters from the input file, automatically expanding the buffer as needed. The function filters out null characters to maintain consistency with the main buffer filling logic.

## Parameters / Member Variables
This function takes no parameters but uses several global variables:
- : Pointer to saved buffer characters for restoration
- : End pointer for the saved buffer area
- : Current position in the lookahead buffer
- : End of valid data in the lookahead buffer  
- : Start of the dynamically allocated lookahead buffer
- : End of the allocated lookahead buffer space
- : Input file stream for reading new characters

## Dependencies
- Functions called/Symbols referenced:
  - malloc (for initial lookahead buffer allocation)
  - realloc (for expanding the lookahead buffer when needed)
  - errx (for fatal error reporting on allocation failure)
- Called from (representative examples):
  - is_func_definition (in lexi.c for parsing function definitions)
  - _discoverArchiveFormat (in pg_backup_archiver.c for archive format detection)
  - _tarReadRaw (in pg_backup_tar.c for tar format processing)

## Notes and Other Information
- Returns the next character as an unsigned char cast to int, or EOF when end of input is reached
- Automatically manages buffer allocation, starting with 64 bytes and doubling as needed
- Null characters are skipped to maintain consistency with main buffer behavior
- Must be paired with lookahead_reset() calls to avoid losing synchronization with the main buffer
- Critical for multi-character lookahead in parsing contexts where token recognition requires examining future characters
- Used extensively in pg_bsd_indent for parsing decisions and in pg_dump utilities for format detection