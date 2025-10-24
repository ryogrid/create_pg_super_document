# find_arguments

## Location
[src/port/snprintf.c:746-963](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/snprintf.c#L746-L963)

## Overview
Validates and extracts variable arguments for printf-style format strings that use positional parameter specifications (%n$).

## Definition

```c
static bool
find_arguments(const char *format, va_list args,
			   PrintfArgValue *argvalues)
```
## Detailed Description
This function analyzes printf-style format strings containing positional parameters (like %1, %2) and extracts the corresponding arguments from a va_list. It performs comprehensive validation to ensure all argument references use consistent positional notation and that argument types match their format specifiers. The function is part of PostgreSQL's portable snprintf implementation and ensures compatibility with C99 positional parameter standards.

The function parses the format string character by character, identifying conversion specifiers and their associated argument positions. It builds an array mapping each position to its expected argument type, then extracts arguments from the va_list in the correct order. This enables format strings to reference arguments out of order (e.g., "%2 %1").

## Parameters / Member Variables
- `*format`: The printf-style format string containing positional parameter specifications
- `args`: Variable argument list (va_list) containing the actual arguments to be formatted
- `*argvalues`: Output array that will be filled with argument values indexed by their positional numbers
## Dependencies
- Functions called/Symbols referenced:
  - PrintfArgValue (struct type)
  - PG_NL_ARGMAX (constant defining maximum number of positional arguments)
  - [PrintfArgType](../P/PrintfArgType.md) (enum type)
  - ATYPE_* constants (ATYPE_INT, ATYPE_LONG, ATYPE_LONGLONG, ATYPE_DOUBLE, ATYPE_CHARPTR, ATYPE_NONE)
  - va_arg (standard C macro for extracting variable arguments)
  - strchr (standard C library function)
  - Max (PostgreSQL macro for maximum value)

- Called from (representative examples):
  - [dopr](../d/dopr.md) (main printf formatting function)
  - [flushbuffer](flushbuffer.md) (output buffer management function)

## Notes and Other Information
- Returns true if the format string is valid and arguments are successfully extracted, false otherwise
- Enforces C99 standard requirement that all argument references must be either positional (%n$) or non-positional, but not mixed
- Supports all standard printf conversion specifiers (d, i, o, u, x, X, c, s, p, e, E, f, g, G, m, %)
- Handles width and precision specifiers, including dynamic ones (*n$)
- Limited to PG_NL_ARGMAX positional arguments to prevent resource exhaustion
- Part of PostgreSQL's platform-independent printf implementation for systems lacking proper C99 support

## Simplified Source

```c
static bool find_arguments(const char *format, va_list args, PrintfArgValue *argvalues)
{
    int ch;
    bool afterstar;
    int accum;
    int longlongflag, longflag;
    int fmtpos;
    int last_dollar = 0;
    PrintfArgType argtypes[PG_NL_ARGMAX + 1] = {0};

    // Parse format string to determine argument types and positions
    while (*format != '\0') {
        // Skip to next conversion specifier
        if (*format != '%') {
            format = strchr(format + 1, '%');
            if (format == NULL) break;
        }

        format++; // Skip '%'
        longflag = longlongflag = 0;
        fmtpos = accum = 0;
        afterstar = false;

        // Parse conversion specifier components
        while ((ch = *format++)) {
            switch (ch) {
                case '-': case '+': continue;
                case '0'...'9':
                    accum = accum * 10 + (ch - '0');
                    continue;
                case '.':
                    accum = 0;
                    continue;
                case '*':
                    if (afterstar) return false; // Previous star missing dollar
                    afterstar = true;
                    accum = 0;
                    continue;
                case '$':
                    // Validate position range
                    if (accum <= 0 || accum > PG_NL_ARGMAX)
                        return false;

                    if (afterstar) {
                        // Star argument - must be int
                        if (argtypes[accum] && argtypes[accum] != ATYPE_INT)
                            return false;
                        argtypes[accum] = ATYPE_INT;
                        last_dollar = Max(last_dollar, accum);
                        afterstar = false;
                    } else {
                        fmtpos = accum;
                    }
                    accum = 0;
                    continue;

                case 'l':
                    if (longflag) longlongflag = 1;
                    else longflag = 1;
                    continue;
                case 'z': /* size_t modifier - handle platform differences */
                    continue;
                case 'h': case '\'': continue; // Ignored

                // Integer conversions
                case 'd': case 'i': case 'o': case 'u': case 'x': case 'X':
                    if (!fmtpos) return false; // Non-dollar spec
                    PrintfArgType atype = longlongflag ? ATYPE_LONGLONG :
                                         longflag ? ATYPE_LONG : ATYPE_INT;
                    if (argtypes[fmtpos] && argtypes[fmtpos] != atype)
                        return false;
                    argtypes[fmtpos] = atype;
                    last_dollar = Max(last_dollar, fmtpos);
                    break;

                case 'c':
                    if (!fmtpos) return false;
                    if (argtypes[fmtpos] && argtypes[fmtpos] != ATYPE_INT)
                        return false;
                    argtypes[fmtpos] = ATYPE_INT;
                    last_dollar = Max(last_dollar, fmtpos);
                    break;

                case 's': case 'p':
                    if (!fmtpos) return false;
                    if (argtypes[fmtpos] && argtypes[fmtpos] != ATYPE_CHARPTR)
                        return false;
                    argtypes[fmtpos] = ATYPE_CHARPTR;
                    last_dollar = Max(last_dollar, fmtpos);
                    break;

                case 'e': case 'E': case 'f': case 'g': case 'G':
                    if (!fmtpos) return false;
                    if (argtypes[fmtpos] && argtypes[fmtpos] != ATYPE_DOUBLE)
                        return false;
                    argtypes[fmtpos] = ATYPE_DOUBLE;
                    last_dollar = Max(last_dollar, fmtpos);
                    break;

                case 'm': case '%':
                    break; // No arguments

                default:
                    return false; // Invalid format
            }
            break; // Exit inner loop after processing conversion spec
        }

        if (afterstar) return false; // Incomplete star spec
    }

    // Extract arguments in physical order
    for (int i = 1; i <= last_dollar; i++) {
        switch (argtypes[i]) {
            case ATYPE_NONE:
                return false; // Missing argument
            case ATYPE_INT:
                argvalues[i].i = va_arg(args, int);
                break;
            case ATYPE_LONG:
                argvalues[i].l = va_arg(args, long);
                break;
            case ATYPE_LONGLONG:
                argvalues[i].ll = va_arg(args, long long);
                break;
            case ATYPE_DOUBLE:
                argvalues[i].d = va_arg(args, double);
                break;
            case ATYPE_CHARPTR:
                argvalues[i].cptr = va_arg(args, char *);
                break;
        }
    }

    return true;
}
```