# text_char

## Location
[src/backend/utils/adt/char.c:204-227](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/char.c#L204-L227)

## Overview
Converts a PostgreSQL text data type to a "char" (single byte character) with support for octal escape sequences and empty string handling.

## Definition
```c
Datum text_char(PG_FUNCTION_ARGS)
```

## Detailed Description
This function converts a PostgreSQL text value to a "char" data type following specific conversion rules. It handles three main cases: (1) If the text contains exactly 4 characters in the format of a backslash followed by three octal digits (\nnn), it converts the octal sequence to its corresponding character value. (2) If the text has at least one character, it takes the first character. (3) If the text is empty, it returns the null character ('\0'). The function uses the same conversion logic as the charin() function but explicitly handles the empty string case.

## Parameters / Member Variables
- `arg1`: The input text value retrieved using PG_GETARG_TEXT_PP(0)
- `ch`: Pointer to the character data within the text using VARDATA_ANY()
- `result`: The resulting character value to be returned

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_TEXT_PP (macro for extracting text argument)
  - VARDATA_ANY (macro for accessing variable-length data)
  - VARSIZE_ANY_EXHDR (macro for getting size excluding header)
  - ISOCTAL (macro to check if character is octal digit)
  - FROMOCTAL (macro to convert octal character to numeric value)
  - PG_RETURN_CHAR (macro for returning char result)

- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL CAST operations)

## Notes and Other Information
- Supports octal escape sequence parsing for special characters (\nnn format)
- Handles empty strings by returning null character
- Uses PostgreSQL's variable-length data access macros for safe text processing
- The octal conversion follows the pattern: (first_digit << 6) + (second_digit << 3) + third_digit
- Used internally by PostgreSQL's type conversion system for text to char casts
- The function follows PostgreSQL's V1 calling convention using the PG_FUNCTION_ARGS interface

## Simplified Source

```c
Datum text_char(PG_FUNCTION_ARGS) {
    text *arg1 = PG_GETARG_TEXT_PP(0);
    char *ch = VARDATA_ANY(arg1);
    char result;

    // Handle octal escape sequences (\ooo format)
    if (VARSIZE_ANY_EXHDR(arg1) == 4 && ch[0] == '\\' &&
        ISOCTAL(ch[1]) && ISOCTAL(ch[2]) && ISOCTAL(ch[3])) {
        result = (FROMOCTAL(ch[1]) << 6) +
                 (FROMOCTAL(ch[2]) << 3) +
                 FROMOCTAL(ch[3]);
    } else if (VARSIZE_ANY_EXHDR(arg1) > 0) {
        // Take first character from non-empty text
        result = ch[0];
    } else {
        // Empty string returns null character
        result = '\0';
    }

    PG_RETURN_CHAR(result);
}
```