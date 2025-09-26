# pg_split_opts

## Location
[src/backend/utils/init/postinit.c:519-576](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/postinit.c#L519-L576)

## Overview
pg_split_opts is a utility function that parses a string of command-line options and splits it into individual arguments, appending them to an argv array.

## Definition
void pg_split_opts(char **argv, int *argcp, const char *optstr)

## Detailed Description
This function takes a string containing multiple command-line options and splits it into individual arguments that are added to an argv-style array. The function handles several important parsing features:

1. **Whitespace Handling**: Skips leading whitespace and uses whitespace as the primary delimiter between options
2. **Escape Sequence Support**: Supports backslash escaping to include literal spaces and backslashes in option values
   -  represents a literal backslash
   -  allows spaces within option values
3. **Dynamic Parsing**: Processes options one at a time, building each argument incrementally
4. **Memory Management**: Uses StringInfo for temporary storage and pstrdup for permanent argument storage

The function is designed to parse option strings that might contain embedded spaces or special characters, making it suitable for processing complex configuration strings.

## Parameters / Member Variables
- : Array of string pointers where parsed arguments will be stored (caller must ensure sufficient space)
- : Pointer to argument count, incremented for each argument added
- : Input string containing options to be parsed

## Dependencies
- Functions called/Symbols referenced:
  - [initStringInfo](../i/initStringInfo.md) (for initializing temporary string buffer)
  - [resetStringInfo](../r/resetStringInfo.md) (for clearing string buffer between arguments)
  - isspace (for whitespace detection)
  - [appendStringInfoChar](../a/appendStringInfoChar.md) (for building argument strings)
  - [pstrdup](pstrdup.md) (for creating permanent copies of arguments)
  - [pfree](pfree.md) (for cleaning up temporary storage)
- Called from:
  - [process_startup_options](process_startup_options.md) (src/backend/utils/init/postinit.c:1291)
  - INIT_PG_OVERRIDE_ROLE_LOGIN (src/include/miscadmin.h:487)

## Notes and Other Information
- This is a public function, accessible throughout the PostgreSQL codebase
- The caller is responsible for ensuring the argv array has sufficient space
- Maximum possible arguments added is (strlen(optstr) + 1) / 2
- Supports escape sequences for including literal backslashes and spaces in arguments
- Uses StringInfo for efficient string building during parsing
- Each parsed argument is allocated with pstrdup and must be freed by the caller
- Commonly used for processing startup options and configuration strings
- Handles empty strings gracefully by doing nothing