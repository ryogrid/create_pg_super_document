# NUM_numpart_to_char

## Location
[src/backend/utils/adt/formatting.c:5620-5809](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L5620-L5809)

## Overview
Formats and writes numeric parts (digits, signs, decimal points) to output strings during TO_CHAR() processing, handling various formatting modes and locale-specific conventions.

## Definition

```c
static void
NUM_numpart_to_char(NUMProc *Np, int id)
```
## Detailed Description
This function is the counterpart to NUM_numpart_from_char, responsible for generating formatted numeric output during TO_CHAR() operations. It handles the complex logic of positioning signs, digits, and decimal points according to PostgreSQL's formatting specifications.

Key responsibilities include:
- Sign placement (pre-sign, post-sign, bracket notation)
- Digit output with proper spacing and zero handling
- Decimal point positioning with locale awareness
- Fill mode (FM) behavior that suppresses leading/trailing spaces
- Roman numeral detection (early return)
- Zero padding and suppression logic
- Handling of various format patterns (NUM_9, NUM_0, NUM_D, NUM_DEC)

The function maintains careful state management to ensure signs are written exactly once at the appropriate position, and coordinates with NUM_processor to generate properly formatted numeric strings.

## Parameters / Member Variables
- : Pointer to NUMProc structure containing formatting state and configuration
  - : Current position in output buffer
  - : Current position in input number string
  - : Flag tracking whether sign has been output
  - : Current position in format pattern
  - : Flag indicating if numeric content has been written
  - : Pointer to last significant digit for FM mode
  - : Sign character ('+', '-', ' ')
- uid=1000(ryo) gid=1000(ryo) groups=1000(ryo),4(adm),20(dialout),24(cdrom),25(floppy),27(sudo),29(audio),30(dip),44(video),46(plugdev),117(netdev),998(ollama),999(docker): Format token identifier (NUM_9, NUM_0, NUM_D, NUM_DEC, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - IS_ROMAN, IS_ZERO, IS_LSIGN, IS_BRACKET, IS_FILLMODE, IS_PREDEC_SPACE, IS_DECIMAL (format flag checking macros)
  - NUM_LSIGN_PRE, NUM_LSIGN_POST (locale sign position constants)
  - strcpy, strlen (string operations)
  - elog (debug logging)
- Called from (representative examples):
  - [NUM_processor](NUM_processor.md) (formatting.c:6035)
  - DCH_ZONED (formatting.c:1080)

## Notes and Other Information
- Central component of TO_CHAR() numeric formatting with complex conditional logic
- Early returns for Roman numeral formatting to avoid conflicts
- Implements PostgreSQL's specific spacing and zero-handling rules
- Handles edge cases like "9.9" → " .1" for predecimal spaces
- Manages both locale-specific and simple sign conventions
- Supports bracket notation where negative numbers appear as <123>
- Fill mode (FM) suppresses unnecessary spaces and trailing zeros
- Coordinates sign placement timing to avoid duplication
- Debug logging available when DEBUG_TO_FROM_CHAR is enabled
- Must handle the complex interaction between zero padding, fill mode, and decimal positioning