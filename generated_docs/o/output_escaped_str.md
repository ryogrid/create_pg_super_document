# output_escaped_str

## Location
src/interfaces/ecpg/preproc/output.c: 200 - 251

## Overview
The output_escaped_str function properly escapes string content for inclusion in generated C code, handling quotes, newlines, backslashes and other special characters.

## Definition
static void output_escaped_str(char *str, bool quoted)

## Detailed Description
This static function is a utility within the ECPG preprocessor that handles the complex task of escaping strings for safe inclusion in generated C code. It processes the input string character by character, applying appropriate escape sequences for special characters like quotes, newlines, and backslashes. The function has special logic for handling quoted strings (removing outer quotes while preserving inner content) and continuation lines (backslash followed by newline). It ensures that SQL statements and other string data can be safely embedded in the generated C code without syntax errors.

## Parameters / Member Variables
- `str`: The input string to be escaped and output
- `quoted`: Boolean flag indicating whether the string is already quoted (affects handling of leading/trailing quotes)

## Dependencies
- Functions called/Symbols referenced:
  - No external function calls (uses standard C library functions like strlen, fputs, fputc)
- Called from (representative examples):
  - output_simple_statement (at src/interfaces/ecpg/preproc/output.c:21)
  - output_statement (at src/interfaces/ecpg/preproc/output.c:154)
  - output_prepare_statement (at src/interfaces/ecpg/preproc/output.c:173 and 175)
  - output_deallocate_prepare_statement (at src/interfaces/ecpg/preproc/output.c:189)

## Notes and Other Information
- Character-by-character processing ensures proper handling of all special cases
- Special quote handling: if quoted=true and string starts/ends with quotes, outer quotes are preserved but not escaped
- Escape sequences applied: \" for quotes, \\n for newlines, \\\\ for backslashes, \\r\n for carriage return+newline
- Continuation line detection: backslashes followed by whitespace and newlines are handled specially to avoid double-escaping
- The function directly outputs to base_yyout (the preprocessor's output file)
- No memory allocation or freeing occurs within this function
- The function is static, indicating it's only used within the same source file