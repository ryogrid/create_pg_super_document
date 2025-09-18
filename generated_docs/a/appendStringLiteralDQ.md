# appendStringLiteralDQ

## Location
src/fe_utils/string_utils.c: 484 - 526

## Overview
Converts a string value to a dollar-quoted literal and appends it to a PQExpBuffer, ensuring proper quoting without character escaping.

## Definition
```c
void appendStringLiteralDQ(PQExpBuffer buf, const char *str, const char *dqprefix)
```

## Detailed Description
This function implements PostgreSQL's dollar-quoting mechanism for string literals. Dollar-quoting allows strings to be quoted without escaping internal characters, making it particularly useful for function bodies, complex strings, or any content that would otherwise require extensive escaping. The function automatically generates a unique delimiter by starting with a base delimiter (constructed from '$' + optional prefix + '$') and adding suffix characters as needed to ensure the delimiter doesn't appear within the string being quoted.

The algorithm ensures compliance with PostgreSQL's dollar-quoting rules by checking that the chosen delimiter (without the trailing '$') does not exist as a substring within the input string. This prevents ambiguous parsing where the string content could be mistaken for the closing delimiter.

## Parameters / Member Variables
- `buf`: Target PQExpBuffer where the dollar-quoted string will be appended
- `str`: The string content to be dollar-quoted (no escaping will be performed on this content)
- `dqprefix`: Optional prefix for the dollar quote delimiter (can be NULL); helps create more readable or context-specific delimiters

## Dependencies
- Functions called/Symbols referenced:
  - appendPQExpBufferChar (used to build delimiter and append characters)
  - appendPQExpBufferStr (used to append string portions)
  - createPQExpBuffer (creates temporary buffer for delimiter construction)
  - destroyPQExpBuffer (cleans up temporary buffer)
  - strstr (searches for delimiter conflicts in input string)
- Called from (representative examples):
  - dumpFunc (in pg_dump.c for function definition dumping)

## Notes and Other Information
- No character escaping is performed on the input string, following dollar-quoting semantics
- The function handles encoding issues transparently since dollar-quoting doesn't require character-level escaping
- Uses a cycling suffix system ('_XXXXXXX') to generate unique delimiters when conflicts are detected
- The delimiter collision detection specifically excludes the trailing '$' to handle edge cases where strings end with potential delimiter patterns
- Commonly used in pg_dump for preserving complex string content like function bodies without modification