# TailMatchesImpl

## Location
[src/bin/psql/tab-complete.c:1537-1569](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L1537-L1569)

## Overview
Implements the core logic for TailMatches and TailMatchesCS macros, checking if the last N words in the command line match a given pattern for tab completion purposes.

## Definition
```c
static bool TailMatchesImpl(bool case_sensitive,
                           int previous_words_count, char **previous_words,
                           int narg, ...)
```

## Detailed Description
This function provides the underlying implementation for the TailMatches and TailMatchesCS macros used throughout psql's tab completion system. It compares the last N words from the command line (stored in reverse order in the previous_words array) against a variable number of pattern arguments. The function supports both case-sensitive and case-insensitive matching depending on the case_sensitive parameter.

The function uses variadic arguments to accept a flexible number of pattern strings to match against. It iterates through these patterns and compares them with the corresponding words from the end of the command line. The array indexing accounts for the fact that previous_words[0] contains the most recent (last) word entered.

## Parameters / Member Variables
- `case_sensitive`: Boolean flag determining whether string comparison should be case-sensitive
- `previous_words_count`: Total number of words in the previous_words array  
- `previous_words`: Array of strings containing words from the command line in reverse order (last word at index 0)
- `narg`: Number of pattern arguments to match against
- `...`: Variable number of const char* arguments representing the patterns to match

## Dependencies
- Functions called/Symbols referenced:
  - [word_matches](../w/word_matches.md) (performs individual word comparison with case sensitivity support)

- Called from (representative examples):
  - TailMatches (macro wrapper for case-insensitive matching)
  - TailMatchesCS (macro wrapper for case-sensitive matching)

## Notes and Other Information
- The function returns false immediately if there aren't enough words to match (previous_words_count < narg)
- Array indexing uses `previous_words[narg - argno - 1]` to correctly map variadic arguments to the tail words
- Uses va_list for handling variable arguments, properly cleaning up with va_end()
- This is a static function, only accessible within the tab-complete.c file
- Critical component of psql's context-aware tab completion system