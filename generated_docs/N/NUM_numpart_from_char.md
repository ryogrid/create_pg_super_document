# NUM_numpart_from_char

## Location
src/backend/utils/adt/formatting.c: 5405 - 5608

## Overview
Extracts numeric parts (digits, signs, decimal points) from input strings during TO_NUMBER() processing, handling locale-specific formatting and various sign conventions.

## Definition


## Detailed Description
This function is a core component of PostgreSQL's TO_NUMBER() functionality, responsible for parsing and extracting numeric components from formatted input strings. It handles complex parsing scenarios including:

- Pre-number sign detection (both locale-specific and simple +/- signs)
- Digit extraction with position tracking (pre/post decimal)
- Decimal point recognition (locale-aware)  
- Post-number sign detection for various formatting patterns
- Bracket notation for negative numbers (< >)
- Fill mode (FM) and exact positioning requirements

The function processes input character by character, maintaining state about what has been read and updating the numeric buffer accordingly. It includes extensive debug logging and boundary checking to handle various edge cases in number parsing.

## Parameters / Member Variables
- : Pointer to NUMProc structure containing parsing state and configuration
  - : Current position in input string
  - : Buffer for constructing parsed number
  - : Current position in number buffer
  - : Count of digits read before decimal point
  - : Count of digits read after decimal point
  - : Flag indicating decimal point has been encountered
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): Format token identifier (NUM_0, NUM_9, NUM_DEC, etc.)
- : Total length of input string for boundary checking

## Dependencies
- Functions called/Symbols referenced:
  - OVERLOAD_TEST (boundary checking macro)
  - IS_LSIGN, IS_DECIMAL, IS_BRACKET, IS_PLUS, IS_MINUS (format flag checking macros)
  - AMOUNT_TEST (input length validation macro)
  - strlen, strncmp (string operations)
  - isdigit (character classification)
  - elog (debug logging)
- Called from (representative examples):
  - NUM_processor (formatting.c:6040)
  - DCH_ZONED (formatting.c:1079)

## Notes and Other Information
- Critical component of TO_NUMBER() parsing with complex state management
- Handles both pre-sign and post-sign scenarios based on format requirements  
- Supports locale-specific signs, decimal points, and thousands separators
- Includes extensive boundary checking to prevent buffer overflows
- Debug logging available when DEBUG_TO_FROM_CHAR is enabled
- Must handle various ambiguous sign positioning scenarios in fill mode (FM)
- Supports bracket notation where '<' represents negative sign
- Carefully manages input position to coordinate with NUM_processor() main loop