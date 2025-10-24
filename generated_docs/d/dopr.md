# dopr

## Location
[src/port/snprintf.c:373-745](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/snprintf.c#L373-L745)

## Overview
The core formatting engine of PostgreSQL's portable printf implementation that parses format strings and converts arguments to their textual representations.

## Definition
```c
static void dopr(PrintfTarget *target, const char *format, va_list args)
```

## Detailed Description
dopr is the heart of PostgreSQL's portable printf implementation, responsible for parsing format strings and orchestrating the conversion of variable arguments into formatted text output. This function implements a comprehensive printf-compatible formatter that handles both traditional printf syntax and POSIX-style positional parameters (\%n$ syntax).

The function processes the format string character by character, identifying literal text (which is output directly) and conversion specifications (which trigger argument processing and formatting). It supports all standard printf conversion specifiers including integers (\%d, \%i, \%o, \%u, \%x, \%X), floating-point (\%e, \%E, \%f, \%g, \%G), characters (\%c), strings (\%s), pointers (\%p), and the special PostgreSQL extension \%m for errno-based error messages.

A key feature of dopr is its support for both traditional printf argument processing (arguments consumed in order) and POSIX positional parameters (\%n$ format), which allows arguments to be referenced by position rather than order. When positional parameters are detected, the function calls find_arguments() to pre-process the entire format string and organize arguments into an indexed array.

The function includes extensive formatting control support, including field width, precision, padding (zero or space), justification (left or right), and sign handling. It also provides optimized fast paths, such as the direct handling of simple \%s conversions without full format parsing.

## Parameters / Member Variables
- `target`: Pointer to PrintfTarget structure containing output buffer, stream, and formatting state information
- `format`: Format string containing literal text and conversion specifications
- `args`: va_list containing the variable arguments to be formatted

## Dependencies
- Functions called/Symbols referenced:
  - strchrnul
  - [dostr](dostr.md)
  - [find_arguments](../f/find_arguments.md)
  - [fmtint](../f/fmtint.md)
  - [fmtchar](../f/fmtchar.md)
  - [fmtstr](../f/fmtstr.md)
  - [fmtptr](../f/fmtptr.md)
  - [fmtfloat](../f/fmtfloat.md)
  - strerror_r
  - [dopr_outch](dopr_outch.md)
- Called from (representative examples):
  - [pg_vsnprintf](../p/pg_vsnprintf.md)
  - [pg_vsprintf](../p/pg_vsprintf.md)
  - [pg_vfprintf](../p/pg_vfprintf.md)

## Notes and Other Information
- This is a static function, only accessible within src/port/snprintf.c
- Supports both traditional printf syntax and POSIX positional parameters (\%n$ format)
- Includes PostgreSQL-specific extensions like \%m for errno-based error messages
- Implements comprehensive error handling, setting target->failed and preserving errno values
- Uses an optimized fast path for simple \%s conversions to improve performance
- Handles all standard printf conversion specifiers with full formatting control
- Supports size modifiers (l, ll, z, h) for integer conversions
- The function is designed to be portable across different platforms and C library implementations
- Critical component in PostgreSQL's strategy to ensure consistent printf behavior across all supported platforms

## Simplified Source

```c
static void dopr(PrintfTarget *target, const char *format, va_list args)
{
    int save_errno = errno;
    const char *first_pct = NULL;
    bool have_dollar = false;
    PrintfArgValue argvalues[PG_NL_ARGMAX + 1];

    // Main format string processing loop
    while (*format != '\0') {
        // Handle literal text (non-% characters)
        if (*format != '%') {
            const char *next_pct = strchrnul(format + 1, '%');
            dostr(format, next_pct - format, target);
            if (target->failed || *next_pct == '\0')
                break;
            format = next_pct;
        }

        // Remember first conversion spec for %n$ processing
        if (first_pct == NULL)
            first_pct = format;

        format++; // Skip '%'

        // Fast path for simple %s
        if (*format == 's') {
            format++;
            char *strvalue = va_arg(args, char *);
            if (strvalue == NULL) strvalue = "(null)";
            dostr(strvalue, strlen(strvalue), target);
            if (target->failed) break;
            continue;
        }

        // Parse conversion spec components
        int fieldwidth = 0, precision = 0, zpad = 0;
        int leftjust = 0, forcesign = 0, fmtpos = 0;
        int longflag = 0, longlongflag = 0, pointflag = 0;
        int accum = 0;
        bool have_star = false, afterstar = false;

        // Parse flags, width, precision, modifiers
        int ch;
        while ((ch = *format++)) {
            switch (ch) {
                case '-': leftjust = 1; continue;
                case '+': forcesign = 1; continue;
                case '0': if (accum == 0 && !pointflag) zpad = '0'; /* fall through */
                case '1'...'9': accum = accum * 10 + (ch - '0'); continue;
                case '.':
                    if (!have_star) fieldwidth = accum;
                    pointflag = 1; accum = 0; continue;
                case '*':
                    // Handle width/precision from argument
                    if (!have_dollar) {
                        int starval = va_arg(args, int);
                        if (pointflag) {
                            precision = starval < 0 ? 0 : starval;
                            if (precision == 0) pointflag = 0;
                        } else {
                            fieldwidth = starval < 0 ? -starval : starval;
                            if (starval < 0) leftjust = 1;
                        }
                    } else {
                        afterstar = true;
                    }
                    have_star = true; accum = 0; continue;
                case '$':
                    // Switch to positional parameter mode
                    if (!have_dollar) {
                        if (!find_arguments(first_pct, args, argvalues))
                            goto bad_format;
                        have_dollar = true;
                    }
                    if (afterstar) {
                        // Process delayed star value
                        int starval = argvalues[accum].i;
                        // ... star processing logic
                        afterstar = false;
                    } else {
                        fmtpos = accum;
                    }
                    accum = 0; continue;
                case 'l':
                    if (longflag) longlongflag = 1;
                    else longflag = 1;
                    continue;
                case 'z': /* size_t modifier */ continue;
                case 'h': case '\'': /* ignored */ continue;

                // Conversion specifiers
                case 'd': case 'i': case 'o': case 'u': case 'x': case 'X': {
                    // Process integer conversion
                    if (!have_star) {
                        if (pointflag) precision = accum;
                        else fieldwidth = accum;
                    }
                    long long numvalue;
                    if (have_dollar) {
                        numvalue = longlongflag ? argvalues[fmtpos].ll :
                                   longflag ? argvalues[fmtpos].l :
                                   argvalues[fmtpos].i;
                    } else {
                        numvalue = longlongflag ? va_arg(args, long long) :
                                   longflag ? va_arg(args, long) :
                                   va_arg(args, int);
                    }
                    // Cast to unsigned for o,u,x,X formats
                    if (ch == 'o' || ch == 'u' || ch == 'x' || ch == 'X') {
                        numvalue = longlongflag ? (unsigned long long)numvalue :
                                   longflag ? (unsigned long)numvalue :
                                   (unsigned int)numvalue;
                    }
                    fmtint(numvalue, ch, forcesign, leftjust, fieldwidth,
                           zpad, precision, pointflag, target);
                    break;
                }
                case 'c': {
                    // Character conversion
                    int cvalue = have_dollar ? argvalues[fmtpos].i : va_arg(args, int);
                    fmtchar((unsigned char)cvalue, leftjust, fieldwidth, target);
                    break;
                }
                case 's': {
                    // String conversion
                    char *strvalue = have_dollar ? argvalues[fmtpos].cptr : va_arg(args, char *);
                    if (strvalue == NULL) strvalue = "(null)";
                    if (!have_star) {
                        if (pointflag) precision = accum;
                        else fieldwidth = accum;
                    }
                    fmtstr(strvalue, leftjust, fieldwidth, precision, pointflag, target);
                    break;
                }
                case 'p': {
                    // Pointer conversion
                    void *ptrvalue = have_dollar ? argvalues[fmtpos].cptr : va_arg(args, void *);
                    fmtptr(ptrvalue, target);
                    break;
                }
                case 'e': case 'E': case 'f': case 'g': case 'G': {
                    // Floating point conversion
                    double fvalue = have_dollar ? argvalues[fmtpos].d : va_arg(args, double);
                    if (!have_star) {
                        if (pointflag) precision = accum;
                        else fieldwidth = accum;
                    }
                    fmtfloat(fvalue, ch, forcesign, leftjust, fieldwidth,
                             zpad, precision, pointflag, target);
                    break;
                }
                case 'm': {
                    // PostgreSQL extension: errno message
                    char errbuf[PG_STRERROR_R_BUFLEN];
                    const char *errm = strerror_r(save_errno, errbuf, sizeof(errbuf));
                    dostr(errm, strlen(errm), target);
                    break;
                }
                case '%':
                    dopr_outch('%', target);
                    break;
                default:
                    goto bad_format;
            }
            break; // Exit format parsing loop after conversion
        }

        if (target->failed)
            break;
    }
    return;

bad_format:
    errno = EINVAL;
    target->failed = true;
}
```