# parse_re_flags

## Location
[src/backend/utils/adt/regexp.c:385-458](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/regexp.c#L385-L458)

## Overview
Parses and validates regular expression flag options from a text string, converting user-specified flags into internal regex compilation flags used by PostgreSQL's regex engine.

## Definition
```c
static void parse_re_flags(pg_re_flags *flags, text *opts)
```

## Detailed Description
This function serves as the flag parsing engine for PostgreSQL's regex functions, converting user-specified option strings into the appropriate internal flags used by Spencer's regex library. It supports a comprehensive set of regex options that control various aspects of pattern matching behavior including case sensitivity, newline handling, regex syntax flavor, and more.

The function initializes flags to sensible defaults (REG_ADVANCED flavor with glob disabled) and then processes each character in the options string to set or clear specific compilation flags. It provides detailed error reporting for invalid flag characters, including proper multibyte character handling for error messages.

## Parameters / Member Variables
- `flags`: Output parameter, a pg_re_flags structure to be filled with parsed options
- `opts`: TEXT object containing the flag string, or NULL to use defaults

## Dependencies
- Functions called/Symbols referenced:
  - VARDATA_ANY, VARSIZE_ANY_EXHDR (text data extraction macros)
  - [pg_mblen](pg_mblen.md) (multibyte character length calculation)
  - [pg_re_flags](pg_re_flags.md) (output structure type)
  - REG_ADVANCED, REG_EXTENDED, REG_ICASE, REG_NEWLINE, REG_NLSTOP, REG_NLANCH, REG_QUOTE, REG_EXPANDED (regex compilation flags)
- Called from (representative examples):
  - [textregexreplace](../t/textregexreplace.md), textregexreplace_extended
  - [regexp_count](../r/regexp_count.md), regexp_instr, regexp_like
  - [regexp_match](../r/regexp_match.md), regexp_matches
  - [regexp_split_to_table](../r/regexp_split_to_table.md), regexp_split_to_array
  - [regexp_substr](../r/regexp_substr.md)

## Notes and Other Information
- Supported flags include:
  - 'g': Global matching (sets glob flag)
  - 'b': Basic Regular Expressions (BRE syntax)
  - 'c': Case sensitive matching
  - 'e': Extended Regular Expressions (ERE syntax)
  - 'i': Case insensitive matching
  - 'm'/'n': Newline affects anchors and character classes
  - 'p': Perl-like newline handling
  - 'q': Literal string (quote) mode
  - 's': Single line mode
  - 't': Tight syntax
  - 'w': Weird newline mode (affects anchors only)
  - 'x': Expanded syntax (allows whitespace and comments)
- This is a static function used internally within regexp.c
- Provides comprehensive error handling with detailed error messages for invalid flags
- The glob flag is handled separately from regex compilation flags
- Default behavior uses REG_ADVANCED (PostgreSQL's enhanced regex flavor)

## Simplified Source

```c
static void
parse_re_flags(pg_re_flags *flags, text *opts)
{
    // Initialize with advanced regex flavor
    flags->cflags = REG_ADVANCED;
    flags->glob = false;

    if (opts)
    {
        char *opt_p = VARDATA_ANY(opts);
        int opt_len = VARSIZE_ANY_EXHDR(opts);

        // Process each flag character
        for (int i = 0; i < opt_len; i++)
        {
            switch (opt_p[i])
            {
                case 'g': flags->glob = true; break;                    // Global matching
                case 'b': flags->cflags &= ~(REG_ADVANCED | REG_EXTENDED | REG_QUOTE); break;  // BRE
                case 'c': flags->cflags &= ~REG_ICASE; break;          // Case sensitive
                case 'e': flags->cflags |= REG_EXTENDED;               // ERE
                         flags->cflags &= ~(REG_ADVANCED | REG_QUOTE); break;
                case 'i': flags->cflags |= REG_ICASE; break;           // Case insensitive
                case 'm':
                case 'n': flags->cflags |= REG_NEWLINE; break;         // Newline affects anchors
                case 'p': flags->cflags |= REG_NLSTOP;                 // Perl-like newlines
                         flags->cflags &= ~REG_NLANCH; break;
                case 'q': flags->cflags |= REG_QUOTE;                  // Literal string
                         flags->cflags &= ~(REG_ADVANCED | REG_EXTENDED); break;
                case 's': flags->cflags &= ~REG_NEWLINE; break;        // Single line
                case 't': flags->cflags &= ~REG_EXPANDED; break;       // Tight syntax
                case 'w': flags->cflags &= ~REG_NLSTOP;                // Weird newlines
                         flags->cflags |= REG_NLANCH; break;
                case 'x': flags->cflags |= REG_EXPANDED; break;        // Expanded syntax
                default:
                    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("invalid regular expression option: \"%.*s\"",
                                   pg_mblen(opt_p + i), opt_p + i)));
            }
        }
    }
}
```