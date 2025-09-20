# setup_text_search

## Location
[src/bin/initdb/initdb.c:2812-2845](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L2812-L2845)

## Overview
Sets up the default text search configuration for a PostgreSQL database during initialization, determining the appropriate configuration based on the system locale.

## Definition

```c
void
setup_text_search(void)
```
## Detailed Description
This function configures the default text search configuration for the PostgreSQL database being initialized. It operates in two modes:

1. **Automatic Detection**: If no default text search configuration is explicitly specified, it attempts to find a suitable configuration that matches the system's LC_CTYPE locale using . If no matching configuration is found, it falls back to the "simple" configuration.

2. **Validation Mode**: If a default text search configuration has already been specified (likely via command-line options), it validates that the specified configuration is appropriate for the current locale and issues warnings if there are potential mismatches.

The function ensures that the database has a valid text search configuration and provides user feedback about the chosen configuration. This is crucial for proper full-text search functionality in PostgreSQL.

## Parameters / Member Variables
This function takes no parameters but operates on global variables:
- : Global variable holding the text search configuration name
- : Global variable containing the LC_CTYPE locale setting

## Dependencies
- Functions called/Symbols referenced:
  - : Finds text search configuration matching a locale
  - : Logs informational messages
  - : Logs warning messages
  - : Outputs final configuration message
- Called from (representative examples):
  - : Called during initdb main execution flow

## Notes and Other Information
- This function is part of the initdb utility, which initializes new PostgreSQL database clusters
- The "simple" configuration is used as a safe fallback when no locale-appropriate configuration can be determined
- Warning messages help administrators identify potential locale/configuration mismatches
- The function always outputs the final chosen configuration to inform the user
- Text search configurations affect how PostgreSQL performs full-text search operations and stemming