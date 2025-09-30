# defGetCopyLogVerbosityChoice

## Location
[src/backend/commands/copy.c:425-462](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/copy.c#L425-L462)

## Overview
defGetCopyLogVerbosityChoice extracts and validates a CopyLogVerbosityChoice value from a DefElem parameter, supporting "default" and "verbose" logging levels for COPY operations.

## Definition

```c
static CopyLogVerbosityChoice
defGetCopyLogVerbosityChoice(DefElem *def, ParseState *pstate)
```
## Detailed Description
This function parses and validates the LOG_VERBOSITY option value for COPY statements. The LOG_VERBOSITY option controls the amount of detail included in log messages generated during COPY operations. It accepts two string values: "default" (standard logging level) and "verbose" (increased logging detail for debugging and monitoring purposes). The function provides case-insensitive string matching and generates descriptive error messages with precise parser position information when invalid values are provided.

## Parameters / Member Variables
- : DefElem structure containing the LOG_VERBOSITY parameter definition and string value
- : ParseState used for generating error messages with accurate source position information

## Dependencies
- Functions called/Symbols referenced:
  - [defGetString](defGetString.md)
  - [pg_strcasecmp](../p/pg_strcasecmp.md)
  - ereport  
  - [parser_errposition](../p/parser_errposition.md)
  - COPY_LOG_VERBOSITY_DEFAULT/VERBOSE constants
- Called from (representative examples):
  - [ProcessCopyOptions](../P/ProcessCopyOptions.md)

## Notes and Other Information
- Accepts only "default" and "verbose" as valid string values with case-insensitive comparison
- Uses parser_errposition to provide precise error location information in source queries
- Returns COPY_LOG_VERBOSITY_DEFAULT as a fallback to keep the compiler quiet, though error reporting should prevent reaching this point
- The "verbose" option increases logging detail, which can be helpful for troubleshooting COPY operations and monitoring data loading progress
- Unlike some other COPY options, LOG_VERBOSITY can be used with both COPY FROM and COPY TO operations

## Simplified Source

```c
static CopyLogVerbosityChoice defGetCopyLogVerbosityChoice(DefElem *def, ParseState *pstate) {
    char *sval = defGetString(def);

    // Check valid verbosity levels
    if (pg_strcasecmp(sval, "default") == 0)
        return COPY_LOG_VERBOSITY_DEFAULT;
    if (pg_strcasecmp(sval, "verbose") == 0)
        return COPY_LOG_VERBOSITY_VERBOSE;

    // Error for invalid values
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                   errmsg("COPY %s \"%s\" not recognized", "LOG_VERBOSITY", sval),
                   parser_errposition(pstate, def->location)));
    return COPY_LOG_VERBOSITY_DEFAULT;
}
```