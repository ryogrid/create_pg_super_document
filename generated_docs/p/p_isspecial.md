# p_isspecial

## Location
[src/backend/tsearch/wparser_def.c:692-1613](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/wparser_def.c#L692-L1613)

## Overview
Determines if the current character in a text parser has special properties in Unicode text processing - specifically, it returns true for characters with zero display length or special signs in several languages that aren't word-breakers but also aren't alphabetic.

## Definition

```c
typedef struct
{
	const TParserStateActionItem *action;	/* the actual state info */
	TParserState state;			/* only for Assert crosscheck */
#ifdef WPARSER_TRACE
	const char *state_name;		/* only for debug printout */
#endif
} TParserStateAction;
```
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
  - [pg_dsplen](pg_dsplen.md) (checks character display length)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md) (gets current database encoding)
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

## Simplified Source

```c
static int p_isspecial(TParser *prs) {
    // Check if character has zero display length (control characters)
    if (pg_dsplen(prs->str + prs->state->posbyte) == 0)
        return 1;

    // For UTF-8 databases, check against Unicode combining marks
    if (GetDatabaseEncoding() == PG_UTF8 && prs->usewide) {
        // Static array of Unicode combining mark characters
        // (includes vowel signs from Devanagari, Bengali, Tamil, etc.)
        static const pg_wchar strange_letter[] = {
            0x0903, 0x093E, 0x093F, 0x0940, // Devanagari signs
            0x0982, 0x0983, 0x09BE, 0x09BF, // Bengali signs
            0x0BBE, 0x0BBF, 0x0BC1, 0x0BC2, // Tamil signs
            // ... (many more Unicode combining marks)
            0xAA33, 0xAA34, 0xAA4D          // Cham signs
        };

        // Get current character
        pg_wchar c = prs->pgwstr ?
            *(prs->pgwstr + prs->state->poschar) :
            (pg_wchar) *(prs->wstr + prs->state->poschar);

        // Binary search in sorted array
        const pg_wchar *low = strange_letter;
        const pg_wchar *high = strange_letter + lengthof(strange_letter);

        while (low < high) {
            const pg_wchar *mid = low + ((high - low) >> 1);
            if (*mid == c)
                return 1;  // Found special character
            else if (*mid < c)
                low = mid + 1;
            else
                high = mid;
        }
    }

    return 0;  // Not a special character
}
```