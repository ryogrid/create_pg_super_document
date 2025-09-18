# scan_profile

## Location
src/tools/pg_bsd_indent/args.c: 198 - 232

## Overview
The scan_profile function parses a profile configuration file to extract indent options, handling C-style comments and whitespace while processing each option found.

## Definition
```c
static void scan_profile(FILE *f)
```

## Detailed Description
This static function implements a lexical scanner for profile configuration files used by the PostgreSQL BSD indent tool. It performs the following operations:

1. Reads the file character by character using getc()
2. Skips C-style comments (/* ... */) by tracking comment state
3. Treats whitespace as option delimiters when not inside comments
4. Accumulates non-whitespace, non-comment characters into option strings
5. Passes each complete option string to set_option() for processing
6. Provides verbose output when the global verbose flag is set

The scanner maintains a simple state machine to handle comment parsing correctly, ensuring that options inside comments are ignored while preserving the ability to parse multiple options from a single line or across multiple lines.

## Parameters / Member Variables
- `f`: A FILE pointer to an open profile configuration file that will be scanned for indent options

## Dependencies
- Functions called/Symbols referenced:
  - getc (standard C library)
  - isspace (standard C library)
  - printf (standard C library, when verbose mode is enabled)
  - [set_option](set_option.md) (processes each parsed option string)
- Called from (representative examples):
  - [set_profile](set_profile.md) (src/tools/pg_bsd_indent/args.c:187, 191)

## Notes and Other Information
- Uses a BUFSIZ-sized buffer to accumulate option strings
- Implements a simple state machine for C-style comment handling with the `comment` variable tracking the start position of comments
- The function is static, indicating it's only used within the args.c compilation unit
- Gracefully handles EOF by returning when no more options are found
- Verbose output helps with debugging profile file parsing
- The comment handling correctly deals with nested comment-like patterns by tracking the exact position where comments begin