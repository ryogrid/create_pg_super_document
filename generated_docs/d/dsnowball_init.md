# dsnowball_init

## Location
src/backend/snowball/dict_snowball.c: 220 - 269

## Overview
This function initializes a Snowball dictionary instance by parsing configuration options and setting up the stemmer module and optional stopword list.

## Definition
```c
Datum dsnowball_init(PG_FUNCTION_ARGS)
```

## Detailed Description
The function serves as the initialization entry point for Snowball text search dictionaries in PostgreSQL. It processes a list of configuration options provided during dictionary creation, specifically handling 'language' and 'stopwords' parameters. The function validates that required parameters are present and that no parameters are duplicated. It allocates and configures a DictSnowball structure that will be used for subsequent text processing operations.

## Parameters / Member Variables
- Function receives PG_FUNCTION_ARGS which contains:
  - `dictoptions`: List of DefElem structures containing configuration parameters

## Dependencies
- Functions called/Symbols referenced:
  - palloc0
  - locate_stem_module
  - readstoplist
  - defGetString
  - lowerstr
  - ereport
  - CurrentMemoryContext
  - PG_GETARG_POINTER
  - PG_RETURN_POINTER
- Called from (representative examples):
  - PostgreSQL text search framework (referenced by MININT)

## Notes and Other Information
- This is a PostgreSQL function that follows the standard PG function calling convention
- The function enforces that exactly one 'language' parameter must be provided
- Multiple 'stopwords' or 'language' parameters result in errors
- The function returns a pointer to the initialized DictSnowball structure
- Memory allocation is done in the current memory context for proper cleanup
- Unrecognized parameters trigger configuration errors with descriptive messages