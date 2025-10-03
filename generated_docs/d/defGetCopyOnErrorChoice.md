# defGetCopyOnErrorChoice

## Location
[src/backend/commands/copy.c:393-424](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copy.c#L393-L424)

## Overview
defGetCopyOnErrorChoice extracts and validates a CopyOnErrorChoice value from a DefElem parameter, supporting "stop" and "ignore" options exclusively for COPY FROM operations.

## Definition

```c
static CopyOnErrorChoice
defGetCopyOnErrorChoice(DefElem *def, ParseState *pstate, bool is_from)
```
## Detailed Description
This function parses and validates the ON_ERROR option value for COPY statements. The ON_ERROR option controls how COPY FROM handles data conversion errors and constraint violations. It accepts two string values: "stop" (the default behavior, which stops execution on first error) and "ignore" (which skips problematic rows and continues processing). This option is strictly limited to COPY FROM operations and will generate an error if used with COPY TO. The function provides precise error reporting with parser position information for better user feedback.

## Parameters / Member Variables
- `*def`: DefElem structure containing the ON_ERROR parameter definition and string value
- `*pstate`: ParseState used for generating error messages with accurate source position information
- `is_from`: Boolean flag indicating whether this is a COPY FROM operation (true) or COPY TO operation (false)
## Dependencies
- Functions called/Symbols referenced:
  - [defGetString](defGetString.md)
  - [pg_strcasecmp](../p/pg_strcasecmp.md)
  - ereport
  - [parser_errposition](../p/parser_errposition.md)
  - COPY_ON_ERROR_STOP/IGNORE constants
- Called from (representative examples):
  - [ProcessCopyOptions](../P/ProcessCopyOptions.md)

## Notes and Other Information
- Restricted exclusively to COPY FROM operations; generates a descriptive error for COPY TO usage attempts
- Only accepts "stop" and "ignore" as valid string values, with case-insensitive comparison
- Uses parser_errposition to provide precise error location information in source queries
- Returns COPY_ON_ERROR_STOP as a compiler-quieting fallback, though error reporting should prevent reaching this point
- The "ignore" option allows COPY FROM to continue processing despite individual row errors, which is useful for bulk data loading scenarios

## Simplified Source

```c
static CopyOnErrorChoice
defGetCopyOnErrorChoice(DefElem *def, ParseState *pstate, bool is_from)
{
    char *sval = defGetString(def);

    // ON_ERROR only valid for COPY FROM operations
    if (!is_from)
        ereport(ERROR, /* COPY TO not supported */);

    // Accept "stop" or "ignore" values
    if (pg_strcasecmp(sval, "stop") == 0)
        return COPY_ON_ERROR_STOP;
    if (pg_strcasecmp(sval, "ignore") == 0)
        return COPY_ON_ERROR_IGNORE;

    // Invalid value provided
    ereport(ERROR, /* unrecognized ON_ERROR value */);
    return COPY_ON_ERROR_STOP;  // Keep compiler quiet
}
```