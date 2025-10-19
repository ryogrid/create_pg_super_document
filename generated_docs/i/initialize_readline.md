# initialize_readline

## Location
[src/bin/psql/tab-complete.c:1404-1463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/tab-complete.c#L1404-L1463)

## Overview
Initializes the readline library for PostgreSQL's psql command-line client, configuring tab completion, word breaks, quoting characters, and other readline-specific settings.

## Definition

```c
void
initialize_readline(void)
```
## Detailed Description
This function sets up the GNU readline library (or compatible libraries like libedit) for use in psql. It configures various readline variables to provide appropriate tab completion behavior for SQL commands and database object names. The function establishes the completion function, sets word break characters for proper parsing, configures filename quoting behavior, and sets limits on completion records.

The function handles platform-specific features through conditional compilation, particularly around filename quoting functionality. It also works around some inconsistencies in different readline library implementations.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [psql_completion](../p/psql_completion.md) (assigned as completion function)
  - [quote_file_name](../q/quote_file_name.md) (conditional, for filename quoting)
  - [dequote_file_name](../d/dequote_file_name.md) (conditional, for filename dequoting) 
  - [pg_malloc](../p/pg_malloc.md) (for allocating filename quote characters)
  - WORD_BREAKS (macro defining word break characters)

- Called from (representative examples):
  - [initializeInput](initializeInput.md) (from src/bin/psql/input.c:355)

## Notes and Other Information
- The function sets completion_max_records to 1000 to limit the number of completion options shown
- Filename quoting is set to include all possible characters (0-255) to ensure proper quoting behavior
- The rl_completer_quote_characters is intentionally limited to single quotes only due to inconsistent library support for double quotes
- Contains workarounds for differences between GNU readline and libedit implementations
- Uses conditional compilation (#ifdef) to handle features that may not be available in all readline implementations

## Simplified Source

```c
void initialize_readline(void) {
    // Set basic readline configuration
    rl_readline_name = (char *) pset.progname;
    rl_attempted_completion_function = psql_completion;

    // Configure filename quoting functions if available
#ifdef USE_FILENAME_QUOTING_FUNCTIONS
    rl_filename_quoting_function = quote_file_name;
    rl_filename_dequoting_function = dequote_file_name;
#endif

    // Set characters that break words during completion
    rl_basic_word_break_characters = WORD_BREAKS;

    // Configure quote characters (only single quotes due to library limitations)
    rl_completer_quote_characters = "'";

    // Set up filename quote characters to include all possible chars
#ifdef HAVE_RL_FILENAME_QUOTE_CHARACTERS
    unsigned char *fqc = (unsigned char *) pg_malloc(256);
    for (int i = 0; i < 255; i++)
        fqc[i] = (unsigned char) (i + 1);
    fqc[255] = '\0';
    rl_filename_quote_characters = (const char *) fqc;
#endif

    // Set completion limit to prevent overwhelming output
    completion_max_records = 1000;
}
```