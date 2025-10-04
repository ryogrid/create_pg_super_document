# parse_test_flags

## Location
[src/test/modules/test_regex/test_regex.c:250-434](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_regex/test_regex.c#L250-L434)

## Overview
parse_test_flags is a static function that parses a text string containing regex compilation and execution flags, converting them into a structured test_re_flags object with appropriate PostgreSQL regex engine flags.

## Definition
static void parse_test_flags(test_re_flags *flags, text *opts)

## Detailed Description
This function parses a string of single-character flags that control regex compilation and execution behavior. It supports a comprehensive set of flags compatible with Tcl's regex testing interface, including standard POSIX flags, PostgreSQL-specific extensions, and special test-only options. The function converts these character flags into the appropriate bit flags used by PostgreSQL's regex engine.

The function supports several categories of flags:
- Standard regex options (case insensitive, global matching, etc.)
- Newline handling modes (p, w, n flags)
- Advanced regex features (expanded syntax, backreferences)
- Execution control flags (start/end anchoring)
- Debug and trace options
- Expected pattern information bits for testing

Default settings match Tcl's defaults with REG_ADVANCED enabled.

## Parameters / Member Variables
- : Output parameter - test_re_flags structure to populate with parsed options
- : TEXT object containing flag characters, or NULL to use defaults

## Dependencies
- Functions called/Symbols referenced:
  - VARDATA_ANY (extracts data from TEXT object)
  - VARSIZE_ANY_EXHDR (gets TEXT object size excluding header)
  - Multiple REG_* constants (regex compilation and execution flags)
  - [pg_mblen](pg_mblen.md) (gets multibyte character length for error reporting)
  - ereport/ERROR (PostgreSQL error reporting)
- Called from (representative examples):
  - [test_regex](../t/test_regex.md) (main regex testing function)

## Notes and Other Information
- This is a static (internal) function within the test_regex module
- Supports extensive flag compatibility with Tcl's regex testing interface
- Handles multibyte characters properly in error messages
- Sets sensible defaults matching Tcl behavior when no options provided
- Many flags (A-U) are for testing expected regex pattern information bits
- Special flags like '!' enable partial matching and '0' enables indices output
- Flag 'c' enables REG_EXPECT mode with automatic partial and indices flags
- Located in src/test/modules/test_regex/test_regex.c:250-434

## Simplified Source

```c
static void parse_test_flags(test_re_flags *flags, text *opts) {
    // Initialize defaults to match Tcl
    int cflags = REG_ADVANCED;
    int eflags = 0;
    long info = 0;

    flags->glob = false;
    flags->indices = false;
    flags->partial = false;

    if (opts) {
        char *opt_p = VARDATA_ANY(opts);
        int opt_len = VARSIZE_ANY_EXHDR(opts);

        // Parse each flag character
        for (int i = 0; i < opt_len; i++) {
            switch (opt_p[i]) {
                case '-': break;  // No-op placeholder
                case '!': flags->partial = true; break;
                case '*': break;  // Unicode test flag - ignored
                case '0': flags->indices = true; break;

                // User-exposed RE options
                case 'g': flags->glob = true; break;              // Global match
                case 'i': cflags |= REG_ICASE; break;            // Case insensitive
                case 'n': cflags |= REG_NEWLINE; break;          // Newline affects ^ $ . [^
                case 'p': cflags |= REG_NLSTOP; cflags &= ~REG_NLANCH; break; // Perl mode
                case 'w': cflags &= ~REG_NLSTOP; cflags |= REG_NLANCH; break; // Weird mode
                case 'x': cflags |= REG_EXPANDED; break;         // Extended syntax

                // Advanced regex flags
                case 'a': cflags |= REG_ADVF; break;
                case 'b': cflags &= ~REG_ADVANCED; break;
                case 'c': // Can-match mode
                    cflags |= REG_EXPECT;
                    flags->partial = true;
                    flags->indices = true;
                    break;
                case 'e': cflags &= ~REG_ADVANCED; cflags |= REG_EXTENDED; break;
                case 'q': cflags &= ~REG_ADVANCED; cflags |= REG_QUOTE; break;
                case 'o': cflags |= REG_NOSUB; break;            // No subexpressions
                case 's': cflags |= REG_BOSONLY; break;          // Start only

                // Execution flags
                case '^': eflags |= REG_NOTBOL; break;          // Not beginning of line
                case '$': eflags |= REG_NOTEOL; break;          // Not end of line

                // Info bits (A-U) for testing expected pattern features
                case 'A': info |= REG_UBSALNUM; break;
                case 'B': info |= REG_UBRACES; break;
                case 'E': info |= REG_UBBS; break;
                case 'H': info |= REG_ULOOKAROUND; break;
                case 'I': info |= REG_UIMPOSSIBLE; break;
                case 'L': info |= REG_ULOCALE; break;
                case 'M': info |= REG_UUNPORT; break;
                case 'N': info |= REG_UEMPTYMATCH; break;
                case 'P': info |= REG_UNONPOSIX; break;
                case 'Q': info |= REG_UBOUNDS; break;
                case 'R': info |= REG_UBACKREF; break;
                case 'S': info |= REG_UUNSPEC; break;
                case 'T': info |= REG_USHORTEST; break;
                case 'U': info |= REG_UPBOTCH; break;

                default:
                    ereport(ERROR,
                            (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                             errmsg("invalid regular expression test option: \"%.*s\"",
                                    pg_mblen(opt_p + i), opt_p + i)));
                    break;
            }
        }
    }

    flags->cflags = cflags;
    flags->eflags = eflags;
    flags->info = info;
}
```