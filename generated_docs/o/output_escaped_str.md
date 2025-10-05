# output_escaped_str

## Location
[src/interfaces/ecpg/preproc/output.c:200-251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/preproc/output.c#L200-L251)

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
  - [output_simple_statement](output_simple_statement.md) (at src/interfaces/ecpg/preproc/output.c:21)
  - [output_statement](output_statement.md) (at src/interfaces/ecpg/preproc/output.c:154)
  - [output_prepare_statement](output_prepare_statement.md) (at src/interfaces/ecpg/preproc/output.c:173 and 175)
  - [output_deallocate_prepare_statement](output_deallocate_prepare_statement.md) (at src/interfaces/ecpg/preproc/output.c:189)

## Notes and Other Information
- Character-by-character processing ensures proper handling of all special cases
- Special quote handling: if quoted=true and string starts/ends with quotes, outer quotes are preserved but not escaped
- Escape sequences applied: \" for quotes, \\n for newlines, \\\\ for backslashes, \\r\n for carriage return+newline
- Continuation line detection: backslashes followed by whitespace and newlines are handled specially to avoid double-escaping
- The function directly outputs to base_yyout (the preprocessor's output file)
- No memory allocation or freeing occurs within this function
- The function is static, indicating it's only used within the same source file

## Simplified Source

```c
static void output_escaped_str(char *str, bool quoted) {
    int i = 0;
    int len = strlen(str);

    // Handle leading quote for quoted strings
    if (quoted && str[0] == '"' && str[len - 1] == '"') {
        i = 1;
        len--;
        fputs("\"", base_yyout);
    }

    // Escape each character as needed
    for (; i < len; i++) {
        if (str[i] == '"')
            fputs("\\\"", base_yyout);
        else if (str[i] == '\n')
            fputs("\\\n", base_yyout);
        else if (str[i] == '\\') {
            // Handle continuation lines
            int j = i;
            do {
                j++;
            } while (str[j] == ' ' || str[j] == '\t');

            if ((str[j] != '\n') && (str[j] != '\r' || str[j + 1] != '\n'))
                fputs("\\\\", base_yyout);
        }
        else if (str[i] == '\r' && str[i + 1] == '\n') {
            fputs("\\\r\n", base_yyout);
            i++;
        }
        else
            fputc(str[i], base_yyout);
    }

    // Handle trailing quote for quoted strings
    if (quoted && str[0] == '"' && str[len] == '"')
        fputs("\"", base_yyout);
}
```