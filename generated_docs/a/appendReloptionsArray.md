# appendReloptionsArray

## Location
[src/fe_utils/string_utils.c:966-1052](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/string_utils.c#L966-L1052)

## Overview
Formats a PostgreSQL reloptions array and appends it to a buffer as properly formatted option name-value pairs.

## Definition

```c
bool
appendReloptionsArray(PQExpBuffer buffer, const char *reloptions,
					  const char *prefix, int encoding, bool std_strings)
```
## Detailed Description
This function processes PostgreSQL relation options (reloptions) arrays and formats them into readable SQL option syntax. It parses the array representation of options, splits each element into name-value pairs, and formats them with appropriate quoting and prefixes.

The function matches the backend's flatten_reloptions() logic from adt/ruleutils.c. Each option is expected to be in "name=value" format, with missing "=" treated as empty values. Option names are formatted with the provided prefix (commonly "" or "toast.") and values are quoted when necessary.

Key behaviors:
- Parses reloptions array using parsePGArray
- Splits each option into name and value components  
- Applies provided prefix to option names
- Uses intelligent quoting (only when needed)
- Handles encoding and standard string settings

## Parameters / Member Variables
- `buffer`: PQExpBuffer to append the formatted options to
- `*reloptions`: String containing the PostgreSQL array of relation options
- `*prefix`: Prefix to prepend to option names (typically "" or "toast.")
- `encoding`: Character encoding for string literal formatting
- `std_strings`: Whether to use standard string literal syntax
## Dependencies
- Functions called/Symbols referenced:
  - [parsePGArray](../p/parsePGArray.md)
  - strchr
  - [appendPQExpBufferStr](appendPQExpBufferStr.md)  
  - [appendPQExpBuffer](appendPQExpBuffer.md)
  - [fmtId](../f/fmtId.md)
  - [appendStringLiteral](appendStringLiteral.md)
  - free
  - strcmp

- Called from (representative examples):
  - [appendReloptionsArrayAH](appendReloptionsArrayAH.md) (src/bin/pg_dump/pg_dump.c:19044)
  - [get_create_object_cmd](../g/get_create_object_cmd.md) (src/bin/psql/command.c:5776)

## Notes and Other Information
- Located in src/fe_utils/string_utils.c:966-1052
- Returns false if reloptions array cannot be parsed, true on success
- Logic designed to match backend's flatten_reloptions() from adt/ruleutils.c
- Uses intelligent quoting strategy - avoids quotes for simple identifiers that don't need them
- Commonly used with empty prefix ("") or "toast." prefix for toast table options
- Part of the pg_dump and psql utilities for handling PostgreSQL relation option formatting