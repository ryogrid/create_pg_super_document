# MatchesImpl

## Location
src/bin/psql/tab-complete.c: 1570 - 1602

## Overview
Implements the core logic for Matches and MatchesCS macros, checking if all words in the command line exactly match a given pattern for tab completion purposes.

## Definition
```c
static bool MatchesImpl(bool case_sensitive,
                       int previous_words_count, char **previous_words,
                       int narg, ...)
```

## Detailed Description
This function provides the underlying implementation for the Matches and MatchesCS macros used in psql's tab completion system. Unlike TailMatchesImpl which matches the last N words, MatchesImpl requires an exact match of all words in the command line against the provided patterns. It enforces that the number of words must exactly equal the number of pattern arguments.

The function performs word-by-word comparison between the command line input (stored in reverse order in previous_words) and the variadic pattern arguments. It supports both case-sensitive and case-insensitive matching modes. The matching fails immediately if the word count doesn't match exactly or if any individual word comparison fails.

## Parameters / Member Variables
- `case_sensitive`: Boolean flag determining whether string comparison should be case-sensitive
- `previous_words_count`: Total number of words in the previous_words array
- `previous_words`: Array of strings containing words from the command line in reverse order (last word at index 0)  
- `narg`: Number of pattern arguments to match against (must equal previous_words_count for success)
- `...`: Variable number of const char* arguments representing the exact patterns to match

## Dependencies
- Functions called/Symbols referenced:
  - [word_matches](../w/word_matches.md) (performs individual word comparison with case sensitivity support)

- Called from (representative examples):
  - Matches (macro wrapper for case-insensitive exact matching)
  - MatchesCS (macro wrapper for case-sensitive exact matching)

## Notes and Other Information
- Returns false immediately if word count doesn't match exactly (previous_words_count != narg)
- Uses the same reverse indexing pattern as TailMatchesImpl: `previous_words[narg - argno - 1]`
- Proper variadic argument handling with va_start/va_end
- This is a static function, only accessible within the tab-complete.c file
- Used for precise command pattern matching where the entire command structure must be known
- More restrictive than TailMatchesImpl since it requires exact word count matching