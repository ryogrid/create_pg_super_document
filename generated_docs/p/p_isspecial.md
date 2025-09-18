# p_isspecial

## Location
src/backend/tsearch/wparser_def.c: 692 - 1613

## Overview
Determines if the current character in a text parser has special properties in Unicode text processing - specifically, it returns true for characters with zero display length or special signs in several languages that aren't word-breakers but also aren't alphabetic.

## Definition


## Detailed Description
This function serves as a specialized character classifier in PostgreSQL's text search word parser. It identifies characters that have unique properties in text tokenization:

1. **Zero-width characters**: Characters that have zero display length according to , which includes control characters and zero-width Unicode characters.

2. **Unicode Mark characters**: When the database encoding is UTF-8 and wide character processing is enabled, it checks against a comprehensive static array of Unicode characters in the 'Mark, Spacing Combining' category. These include:
   - Various vowel signs and marks from Indic scripts (Devanagari, Bengali, Gurmukhi, Gujarati, Oriya, Tamil, Telugu, Kannada, Malayalam)
   - Sinhala vowel signs and marks
   - Tibetan signs
   - Myanmar vowel signs and tone marks
   - Khmer vowel signs
   - Various other Asian script combining marks

The function uses binary search on the sorted array of special Unicode characters for efficient lookup when processing UTF-8 text.

## Parameters / Member Variables
- : Pointer to TParser structure containing the current parsing state, including position information and character data

## Dependencies
- Functions called/Symbols referenced:
  - pg_dsplen (checks character display length)
  - GetDatabaseEncoding (gets current database encoding)
  - PG_UTF8 (UTF-8 encoding constant)
  - lengthof (macro for array length)
- Called from (representative examples):
  - Used within the parser state machine in lines 991, 1013, 1020, 1038 for word boundary detection
  - Referenced in hyphen processing states around lines 1510-1599

## Notes and Other Information
- This function is critical for proper tokenization of multilingual text, especially for languages with complex writing systems
- The extensive Unicode character list suggests this was carefully crafted to handle proper text segmentation across many languages
- Only performs Unicode-specific checks when database encoding is UTF-8 and wide character processing is enabled
- Uses binary search for efficient character lookup in the large Unicode character array