# r_mark_yken

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_turkish.c:893-900](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_turkish.c#L893-L900)

## Overview
Identifies and marks the Turkish temporal suffix -yken (meaning 'while' or 'when') in the Snowball stemming algorithm for Turkish text processing.

## Definition

```c
}

static int r_mark_yken(struct SN_env * z)
```
## Detailed Description
This function is part of the Turkish Snowball stemmer that identifies the temporal suffix "-ken" which can be optionally preceded by the consonant 'y' (making it "-yken"). The suffix "-yken" in Turkish indicates simultaneous action or state, equivalent to "while" or "when" in English (e.g., "okurken" - while reading). 

The function performs exact backward string matching to identify the "ken" suffix and then applies the optional 'y' consonant marking rules that are characteristic of Turkish phonological processes.

## Parameters / Member Variables
- : Pointer to the Snowball environment structure containing:
  - : Current cursor position in the string  
  - : Left boundary limit for matching
  - : Pointer to the character array being processed

## Dependencies
- Functions called/Symbols referenced:
  - : Performs exact backward string matching
  - : Handles 'y' consonant insertion rules
  - : String constant containing "ken"

- Called from (representative examples):
  - : Main suffix processing function

## Notes and Other Information
- Returns 1 on successful match, 0 on failure, negative values for errors
- Matches exactly 3 characters ("ken") working backwards from current position
- Part of Turkish temporal/adverbial suffix recognition
- The 'y' consonant may be inserted between the stem and suffix based on phonological rules
- Used to identify gerund forms and temporal clauses in Turkish text