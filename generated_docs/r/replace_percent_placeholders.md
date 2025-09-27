# replace_percent_placeholders

## Location
[src/common/percentrepl.c:59-137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/percentrepl.c#L59-L137)

## Overview
A utility function that replaces percent-letter placeholders in strings with supplied values, primarily used for GUC parameters like archive_command.

## Definition

```c
char *
replace_percent_placeholders(const char *instr, const char *param_name, const char *letters,...)
```
## Detailed Description
 processes an input string and replaces percent-encoded placeholders (like %f, %b) with corresponding values provided as variadic arguments. The function is designed for scenarios where all replacement values are readily available or cheap to compute, and most invocations will use most values.

The function performs the following operations:
- Scans the input string character by character
- When encountering '%', processes escape sequences:
  - '%%' becomes a single '%'
  - '%<letter>' is replaced with the corresponding value from the variadic arguments
  - '%' at end of string triggers an error
  - Unrecognized placeholders trigger an error
- Returns a palloc'd string with all substitutions made

The function is optimized for use cases like archive_command where string templates need dynamic value substitution. It supports both frontend and backend error reporting mechanisms.

## Parameters / Member Variables
- : The input string containing percent placeholders to be replaced
- : Name of the underlying GUC parameter (used for error reporting)
- : String containing the placeholder letters in the same order as the variadic arguments
- : Variadic arguments containing the replacement values (char*) corresponding to each letter in the letters parameter

## Dependencies
- Functions called/Symbols referenced:
  -  - [Initialize](../I/Initialize.md) StringInfo buffer for result
  -  - [Append](../A/Append.md) single character to result buffer
  -  - [Append](../A/Append.md) string to result buffer
  -  (FRONTEND) /  (backend) - Error reporting
  -  (FRONTEND) - Detailed error reporting
  - , ,  - Variadic argument handling

- Called from (representative examples):
  -  - For recovery command placeholder substitution
  -  - For archive command processing
  -  - For SSL passphrase command processing
  -  - For restore command construction

## Notes and Other Information
- The function returns a palloc'd string that must be freed by the caller
- NULL values in the variadic arguments are treated as unsupported placeholders and trigger errors
- Error handling differs between frontend (pg_log_error + exit) and backend (ereport with ERROR)
- The function is specifically designed for GUC parameters but could potentially be extended for other use cases
- All replacement values must be strings (char*) - the function doesn't support mixed data types
- The letters parameter and variadic arguments must correspond exactly in order and count
- Located in src/common/percentrepl.c:59-137, making it available to both frontend and backend code

## Simplified Source

```c
// Simplified version of replace_percent_placeholders
char *replace_percent_placeholders(const char *instr, const char *param_name,
                                   const char *letters, ...) {
    StringInfoData result;
    initStringInfo(&result);

    // Process input string character by character
    for (const char *sp = instr; *sp; sp++) {
        if (*sp == '%') {
            if (sp[1] == '%') {
                // Convert %% to single %
                sp++;
                appendStringInfoChar(&result, *sp);
            }
            else if (sp[1] == '\0') {
                // Error: incomplete escape sequence
#ifdef FRONTEND
                pg_log_error("invalid value for parameter \"%s\": \"%s\"", param_name, instr);
                pg_log_error_detail("String ends unexpectedly after escape character \"%%\".");
                exit(1);
#else
                ereport(ERROR,
                        (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                         errmsg("invalid value for parameter \"%s\": \"%s\"", param_name, instr),
                         errdetail("String ends unexpectedly after escape character \"%%\".")));
#endif
            }
            else {
                // Look up placeholder character
                bool found = false;
                va_list ap;
                sp++;

                va_start(ap, letters);
                for (const char *lp = letters; *lp; lp++) {
                    char *val = va_arg(ap, char *);

                    if (*sp == *lp) {
                        if (val) {
                            appendStringInfoString(&result, val);
                            found = true;
                        }
                        // If val is NULL, treat as unsupported placeholder
                        break;
                    }
                }
                va_end(ap);

                if (!found) {
                    // Error: unknown placeholder
#ifdef FRONTEND
                    pg_log_error("invalid value for parameter \"%s\": \"%s\"", param_name, instr);
                    pg_log_error_detail("String contains unexpected placeholder \"%%%c\".", *sp);
                    exit(1);
#else
                    ereport(ERROR,
                            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                             errmsg("invalid value for parameter \"%s\": \"%s\"", param_name, instr),
                             errdetail("String contains unexpected placeholder \"%%%c\".", *sp)));
#endif
                }
            }
        }
        else {
            // Regular character: copy as-is
            appendStringInfoChar(&result, *sp);
        }
    }

    return result.data;
}
```

Key simplifications made:
- Organized into clear sections with descriptive comments
- Simplified the conditional flow while preserving all error cases
- Maintained platform-specific error handling (frontend vs backend)
- Consolidated placeholder lookup logic
- Preserved variadic argument handling and NULL value detection
- Focused on the core string processing and substitution algorithm