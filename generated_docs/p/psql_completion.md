# psql_completion

## Location
src/bin/psql/tab-complete.c: 1651 - 1671

## Overview
The main tab completion function for PostgreSQL's psql command-line client that provides context-aware SQL and psql command completions based on the current input line.

## Definition


## Detailed Description
The `psql_completion` function is the central completion handler for psql's readline-based tab completion system. It parses the current command line input to understand the context and provides appropriate completions for SQL commands, database objects, psql backslash commands, and variable interpolations. The function implements a comprehensive state machine that analyzes previous words to determine what type of completion should be offered. It supports complex SQL syntax awareness, including multi-word commands, nested structures, and schema-qualified names. The function integrates with PostgreSQL's system catalogs to provide dynamic completions for database objects like tables, columns, functions, and users.

## Parameters / Member Variables
- `text`: The current word being completed (the text at the cursor position)
- `start`: The starting position of the current word in the readline buffer
- `end`: The ending position of the current word in the readline buffer

## Dependencies
- Functions called/Symbols referenced:
  - get_previous_words (parses command line into word array)
  - COMPLETE_WITH_LIST_CS (completion macro for case-sensitive lists)
  - complete_from_variables (handles variable interpolation completion)
  - TailMatches, HeadMatches, Matches (pattern matching macros)
  - Various completion helper functions for different SQL constructs
- Called from (representative examples):
  - initialize_readline (registered as readline completion function)

## Notes and Other Information
- Function spans approximately 3400+ lines, making it one of the largest functions in PostgreSQL
- Uses extensive macro system (TailMatches, HeadMatches, etc.) for pattern matching against previous words
- Supports completion for all major SQL commands (CREATE, ALTER, DROP, SELECT, etc.)
- Handles psql backslash commands (\d, \dt, \l, etc.)
- Provides schema-aware completion for database objects
- Integrates with PostgreSQL system catalogs for dynamic object name completion
- Returns results in readline's completion format (char** array)
- Manages memory allocation/deallocation for completion results
- Critical component enabling psql's user-friendly interactive SQL editing experience