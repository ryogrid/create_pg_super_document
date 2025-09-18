# parse_test_flags

## Location
src/test/modules/test_regex/test_regex.c: 250 - 434

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