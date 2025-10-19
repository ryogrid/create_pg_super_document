# r_exception1

## Location
[src/backend/snowball/libstemmer/stem_UTF_8_english.c:878-945](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/snowball/libstemmer/stem_UTF_8_english.c#L878-L945)

## Overview
Handles the first set of exceptional English words that require special transformation rather than standard stemming rules, converting irregular forms to their proper base forms.

## Definition
```c
static int r_exception1(struct SN_env * z)
```

## Detailed Description
This function implements Exception List 1 for the English Porter stemming algorithm. It identifies 18 specific irregular English words and transforms them to their correct base forms rather than applying standard stemming rules. The function performs forward pattern matching from the beginning of the word and only processes complete words (cursor must be at end of word after matching).

The exceptional words and their transformations include:
- "andes" → "ski", "atlas" → "sky" (unchanged), "bias" (unchanged), "cosmos" (unchanged)
- "dying" → "die", "early" → "earli", "gently" → "gentl", "howe" (unchanged)  
- "idly" → "idl", "lying" → "lie", "news" (unchanged), "only" → "onli"
- "singly" → "singl", "skies" → "ski", "skis" → "ski", "sky" (unchanged)
- "tying" → "tie", "ugly" → "ugli"

This handling is essential because these words would be incorrectly processed by normal morphological rules.

## Parameters / Member Variables
- `z`: Pointer to the SN_env structure containing the Snowball environment with the word being processed and cursor positions

## Dependencies
- Functions called/Symbols referenced:
  - [find_among](../f/find_among.md): Searches forward through the word for matching patterns from the a_10 array
  - [slice_from_s](../s/slice_from_s.md): Replaces the matched word with the specified replacement string
  - a_10: Array of 18 exceptional word patterns with corresponding action codes
  - s_27 through s_37: Replacement strings for the transformed word forms
- Called from (representative examples):
  - [english_ISO_8859_1_stem](../e/english_ISO_8859_1_stem.md): Main English stemming function (called early to catch exceptions)
  - [english_UTF_8_stem](../e/english_UTF_8_stem.md): UTF-8 version of English stemming

## Notes and Other Information
- This is part of the exception handling system, called before standard stemming rules
- Uses forward matching (find_among) rather than backward matching unlike most stemming steps
- Only processes complete words to avoid false matches within longer words
- Returns 1 if an exception is found and handled, 0 otherwise
- The transformations preserve semantic meaning while normalizing irregular morphological variations
- Essential for accuracy with English words that have irregular plural forms or other non-standard morphology
- Works in conjunction with r_exception2 to provide comprehensive exception handling

## Simplified Source

```c
static int r_exception1(struct SN_env * z) {
    // Mark start position for pattern matching
    z->bra = z->c;

    // Quick character check for efficiency - check 3rd character
    if (z->c + 2 >= z->l || z->p[z->c + 2] >> 5 != 3 ||
        !((42750482 >> (z->p[z->c + 2] & 0x1f)) & 1)) {
        return 0; // Quick reject based on character pattern
    }

    // Find matching exception from the predefined list
    int among_var = find_among(z, a_10, 18);
    if (!among_var) return 0;

    z->ket = z->c;

    // Must be a complete word (cursor at end of word)
    if (z->c < z->l) return 0;

    // Apply appropriate transformation for each exception
    switch (among_var) {
        case 1:  return slice_from_s(z, 3, s_27); // -> "ski"
        case 2:  return slice_from_s(z, 3, s_28); // -> "sky"
        case 3:  return slice_from_s(z, 3, s_29); // -> "die"
        case 4:  return slice_from_s(z, 3, s_30); // -> "lie"
        case 5:  return slice_from_s(z, 3, s_31); // -> "tie"
        case 6:  return slice_from_s(z, 3, s_32); // -> "idl"
        case 7:  return slice_from_s(z, 5, s_33); // -> "gentl"
        case 8:  return slice_from_s(z, 4, s_34); // -> "ugli"
        case 9:  return slice_from_s(z, 5, s_35); // -> "earli"
        case 10: return slice_from_s(z, 4, s_36); // -> "onli"
        case 11: return slice_from_s(z, 5, s_37); // -> "singl"
    }
    return 1;
}
```