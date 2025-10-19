# complete_from_files

## Location
[src/bin/psql/tab-complete.c:5798-5897](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L5798-L5897)

## Overview
Provides filename completion functionality for psql tab completion, handling proper quoting and unquoting of filenames based on the command context and user input.

## Definition

```c
struct stat statbuf;
```
## Detailed Description
This function wraps rl_filename_completion_function() to handle filename completion with proper quoting for psql commands. It strips quotes from input before searching for matches and re-quotes results as needed based on the consuming command's requirements. The function supports two different implementation paths: one using readline's filename quoting hooks (when available) and a fallback implementation that manually handles quoting/unquoting.

For directories, it replaces trailing quotes with slashes for better usability. The function is aware of escape characters and force-quote settings that vary between different psql commands (e.g., \copy has no escape character while other backslash commands use backslash as escape).

## Parameters / Member Variables
- : The input text being completed (potentially quoted filename fragment)
- : Completion state (0 for first call, non-zero for subsequent calls)

## Dependencies
- Functions called/Symbols referenced:
  - rl_filename_completion_function
  - [strtokx](../s/strtokx.md)
  - [quote_if_needed](../q/quote_if_needed.md)
  - S_ISDIR
  - [stat](../s/stat.md)
- Called from (representative examples):
  - HeadMatchesCS (multiple locations in tab-complete.c)
  - THING_NO_SHOW completion generator

## Notes and Other Information
The function behavior depends on global variables completion_charp (escape character) and completion_force_quote (whether to force quotes). It handles both USE_FILENAME_QUOTING_FUNCTIONS and fallback modes for different readline library versions. Special handling exists for directory completion by replacing trailing quotes with forward slashes.

## Simplified Source

```c
static char *
complete_from_files(const char *text, int state)
{
#ifdef USE_FILENAME_QUOTING_FUNCTIONS
    // Modern readline with filename quoting hooks

#ifdef HAVE_RL_COMPLETION_SUPPRESS_QUOTE
    rl_completion_suppress_quote = 1;  // Prevent incorrect quote appending
#endif

    // Force quoting if user started with a quote
    if (*text == '\'')
        completion_force_quote = true;

    return rl_filename_completion_function(text, state);

#else
    // Fallback implementation for older readline
    static const char *unquoted_text;
    char *unquoted_match;
    char *ret = NULL;

    // Force quoting if user started with a quote
    if (*text == '\'')
        completion_force_quote = true;

    if (state == 0) {
        // First call: strip quotes from input text
        unquoted_text = strtokx(text, "", NULL, "'", *completion_charp,
                               false, true, pset.encoding);
        if (!unquoted_text) {
            unquoted_text = text;  // Handle empty string
        }
    }

    // Get filename match using unquoted text
    unquoted_match = rl_filename_completion_function(unquoted_text, state);
    if (unquoted_match) {
        struct stat statbuf;
        bool is_dir = (stat(unquoted_match, &statbuf) == 0 &&
                      S_ISDIR(statbuf.st_mode) != 0);

        // Re-quote the result if needed
        ret = quote_if_needed(unquoted_match, " \t\r\n\"`",
                             '\'', *completion_charp,
                             completion_force_quote, pset.encoding);
        if (ret)
            free(unquoted_match);
        else
            ret = unquoted_match;

        // For directories: replace trailing quote with slash
        if (*ret == '\'' && is_dir) {
            char *retend = ret + strlen(ret) - 1;
            *retend = '/';
            rl_completion_append_character = '\0';
        }
    }

    return ret;
#endif
}
```