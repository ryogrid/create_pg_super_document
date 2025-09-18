# initialize_readline

## Location
src/bin/psql/tab-complete.c: 1404 - 1463

## Overview
Initializes the readline library for PostgreSQL's psql command-line client, configuring tab completion, word breaks, quoting characters, and other readline-specific settings.

## Definition


## Detailed Description
This function sets up the GNU readline library (or compatible libraries like libedit) for use in psql. It configures various readline variables to provide appropriate tab completion behavior for SQL commands and database object names. The function establishes the completion function, sets word break characters for proper parsing, configures filename quoting behavior, and sets limits on completion records.

The function handles platform-specific features through conditional compilation, particularly around filename quoting functionality. It also works around some inconsistencies in different readline library implementations.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - psql_completion (assigned as completion function)
  - quote_file_name (conditional, for filename quoting)
  - dequote_file_name (conditional, for filename dequoting) 
  - pg_malloc (for allocating filename quote characters)
  - WORD_BREAKS (macro defining word break characters)

- Called from (representative examples):
  - initializeInput (from src/bin/psql/input.c:355)

## Notes and Other Information
- The function sets completion_max_records to 1000 to limit the number of completion options shown
- Filename quoting is set to include all possible characters (0-255) to ensure proper quoting behavior
- The rl_completer_quote_characters is intentionally limited to single quotes only due to inconsistent library support for double quotes
- Contains workarounds for differences between GNU readline and libedit implementations
- Uses conditional compilation (#ifdef) to handle features that may not be available in all readline implementations