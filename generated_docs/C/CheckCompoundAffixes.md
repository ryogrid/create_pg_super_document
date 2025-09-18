# CheckCompoundAffixes

## Location
[src/backend/tsearch/spell.c:2294-2335](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tsearch/spell.c#L2294-L2335)

## Overview
Checks for compound affixes in a word, supporting both exact position matching and substring search modes to identify affix boundaries.

## Definition
```c
static int CheckCompoundAffixes(CMPDAffix **ptr, char *word, int len, bool CheckInPlace)
```

## Detailed Description
CheckCompoundAffixes searches for compound affixes within a word to help identify word boundaries in compound word processing. The function operates in two modes based on the CheckInPlace parameter:

1. **CheckInPlace mode (true)**: Performs exact prefix matching from the beginning of the word using strncmp
2. **Substring search mode (false)**: Uses strstr to find the affix anywhere within the word

When an affix is found, the function calculates the appropriate length to return based on whether the affix is a suffix or prefix. For suffixes, it returns the position where the affix ends; for prefixes, it returns 0 to indicate the beginning of the word.

## Parameters / Member Variables
- `ptr`: Pointer to array of compound affixes to check (modified during iteration)
- `word`: Input word to search for affixes
- `len`: Length of the input word
- `CheckInPlace`: Boolean flag controlling search mode (exact prefix vs substring search)

## Dependencies
- Functions called/Symbols referenced:
  - strncmp (string comparison for exact matching)
  - strstr (substring search)
  - CMPDAffix structure fields (affix, len, issuffix)
- Called from (representative examples):
  - SplitToVariants (at line 2399)

## Notes and Other Information
- Returns -1 if no matching affix is found
- Returns the calculated position (suffix end position or 0 for prefix) when an affix is found
- Modifies the ptr parameter by advancing through the affix array
- Part of PostgreSQL's text search compound word processing functionality
- The function handles both prefix and suffix compound affixes differently in its return value calculation