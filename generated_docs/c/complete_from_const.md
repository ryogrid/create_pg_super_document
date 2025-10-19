# complete_from_const

## Location
[src/bin/psql/tab-complete.c:5705-5729](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L5705-L5729)

## Overview
Returns a single fixed string for PostgreSQL's psql tab completion system, primarily used to force a specific completion or prevent readline's default filename completion.

## Definition
```c
static char *complete_from_const(const char *text, int state)
```

## Detailed Description
This function is designed to return exactly one fixed string during tab completion, regardless of what the user has typed. It serves two main purposes: when the completion string is empty, it prevents readline from performing unwanted filename completion; when the completion string is non-empty, it forces replacement of user input with the predetermined string. The function only returns a value on the first call (state == 0) and returns NULL on subsequent calls, indicating no more completions are available.

## Parameters / Member Variables
- `text`: The input text from the user (used for case adjustment when case-insensitive)
- `state`: The state counter used by readline (0 for first call, incremented for subsequent calls)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_strdup](../p/pg_strdup.md) (for duplicating the completion string)
  - [pg_strdup_keyword_case](../p/pg_strdup_keyword_case.md) (for case-adjusted string duplication)
- Global variables used:
  - completion_charp (the single string to be returned)
  - completion_case_sensitive (case sensitivity flag)
- Called from (representative examples):
  - COMPLETE_WITH_CONST macro
  - THING_NO_SHOW macro

## Notes and Other Information
- Returns the completion string only on the first call (state == 0)
- When completion_charp is an empty string, effectively disables readline's filename completion
- When completion_charp is non-empty, will replace user input entirely, potentially overwriting "misspellings"
- The documentation suggests using complete_from_list with a single-element list instead for most use cases
- Supports case adjustment based on psql's case sensitivity settings
- The behavior of overwriting user input makes this function suitable only when there's high certainty about what should appear

## Simplified Source

```c
static char *complete_from_const(const char *text, int state)
{
    Assert(completion_charp != NULL);

    // Only return something on the first call
    if (state == 0)
    {
        if (completion_case_sensitive)
            return pg_strdup(completion_charp);
        else
            // Adjust case according to psql settings for case-insensitive mode
            return pg_strdup_keyword_case(completion_charp, text);
    }
    else
        return NULL;  // No more completions available
}
```