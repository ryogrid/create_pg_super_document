# appendStringInfoRegexpSubstr

## Location
src/backend/utils/adt/varlena.c: 4106 - 4205

## Overview
A static helper function that processes replacement text containing regular expression back references and escape sequences, appending the processed result to a StringInfo buffer.

## Definition


## Detailed Description
This function implements the core logic for regexp replacement text processing in PostgreSQL. It scans through replacement text character by character, handling escape sequences and back references:

- **\1-\9**: Substitutes captured groups from the regular expression match
- **\&**: Substitutes the entire matched string  
- **\\**: Outputs a literal backslash
- **Other escapes**: Treated as literal text (backslash + character)

The function carefully handles character vs. byte positioning, using helper functions to convert between character lengths and byte lengths for proper Unicode support. It processes the replacement text in chunks, copying literal text segments efficiently and substituting back references as needed.

## Parameters / Member Variables
- : StringInfo buffer to append the processed replacement text to
- : Input text containing escape sequences and back references
- : Array of regmatch_t structures containing match positions for captured groups
- : Pointer to the beginning of the match in the source string  
- : Logical character position of start_ptr in the source string

## Dependencies
- Functions called/Symbols referenced:
  - VARDATA_ANY (macro for accessing text data)
  - VARSIZE_ANY_EXHDR (macro for getting text size excluding header)
  - memchr (C library function for finding characters)
  - appendBinaryStringInfo (append binary data to StringInfo)
  - appendStringInfoChar (append single character to StringInfo)
  - charlen_to_bytelen (convert character length to byte length)
  - regmatch_t (POSIX regex match structure)
- Called from (representative examples):
  - replace_text_regexp

## Notes and Other Information
- This is a static function internal to varlena.c, specifically designed for regexp replacement operations
- Handles Unicode properly by distinguishing between character and byte positions
- Gracefully handles edge cases like escapes at the end of strings
- The function assumes pmatch[0] contains the full match, and pmatch[1-9] contain captured groups
- Located in src/backend/utils/adt/varlena.c:4106-4205