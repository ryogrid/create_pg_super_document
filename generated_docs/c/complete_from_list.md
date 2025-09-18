# complete_from_list

## Location
[src/bin/psql/tab-complete.c:5625-5704](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L5625-L5704)

## Overview
Returns matching strings from a fixed, NULL-terminated list of strings for PostgreSQL's psql tab completion system, supporting both case-sensitive and case-insensitive matching.

## Definition
```c
static char *complete_from_list(const char *text, int state)
```

## Detailed Description
This function is a core component of PostgreSQL's psql tab completion system that iterates through a predefined list of strings to find matches for user input. It implements a two-pass matching strategy: first attempting case-sensitive matching, and if no matches are found, falling back to case-insensitive matching. The function maintains static state variables to support readline's completion interface, which calls the function repeatedly with incrementing state values to retrieve all possible matches.

## Parameters / Member Variables
- `text`: The input text to match against the completion list
- `state`: The state counter used by readline (0 for first call, incremented for subsequent calls)

## Dependencies
- Functions called/Symbols referenced:
  - strlen (for calculating text length)
  - strncmp (for case-sensitive string comparison)
  - [pg_strncasecmp](../p/pg_strncasecmp.md) (for case-insensitive string comparison)
  - [pg_strdup](../p/pg_strdup.md) (for duplicating matched strings)
  - [pg_strdup_keyword_case](../p/pg_strdup_keyword_case.md) (for case-adjusted string duplication)
  - [complete_from_list](complete_from_list.md) (recursive call for case-insensitive retry)
- Global variables used:
  - completion_charpp (the list of completion strings)
  - completion_case_sensitive (case sensitivity flag)
- Called from (representative examples):
  - COMPLETE_WITH_LIST_INT macro
  - THING_NO_SHOW macro

## Notes and Other Information
- Uses static variables to maintain state between calls, which is required for readline's completion interface
- Implements a fallback mechanism from case-sensitive to case-insensitive matching
- Returns dynamically allocated strings that must be freed by the caller
- The function expects completion_charpp to be set to a valid NULL-terminated string array
- Supports keyword case adjustment based on psql's case preferences
- Returns NULL when no more matches are available