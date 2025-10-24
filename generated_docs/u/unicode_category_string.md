# unicode_category_string

## Location
[src/common/unicode_category.c:332-405](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_category.c#L332-L405)

## Overview
Converts a Unicode general category enumeration value into its corresponding human-readable string representation.

## Definition

```c
const char *
unicode_category_string(pg_unicode_category category)
```
## Detailed Description
This function provides a string description for Unicode general category values as defined in the Unicode standard. It takes a pg_unicode_category enumeration value and returns a descriptive string that matches the official Unicode category names. The function covers all standard Unicode general categories including letters, marks, numbers, punctuation, symbols, separators, and other character types. This is primarily used for debugging, testing, and user-facing displays where category names need to be presented in readable form.

## Parameters / Member Variables
- `category`: The pg_unicode_category enumeration value to convert to a string representation
## Dependencies
- Functions called/Symbols referenced:
  - [pg_unicode_category](../p/pg_unicode_category.md) (parameter type)
  - All PG_U_* category constants (PG_U_UNASSIGNED, PG_U_UPPERCASE_LETTER, PG_U_LOWERCASE_LETTER, PG_U_TITLECASE_LETTER, PG_U_MODIFIER_LETTER, PG_U_OTHER_LETTER, PG_U_NONSPACING_MARK, PG_U_ENCLOSING_MARK, PG_U_SPACING_MARK, PG_U_DECIMAL_NUMBER, PG_U_LETTER_NUMBER, PG_U_OTHER_NUMBER, PG_U_SPACE_SEPARATOR, PG_U_LINE_SEPARATOR, PG_U_PARAGRAPH_SEPARATOR, PG_U_CONTROL, PG_U_FORMAT, PG_U_PRIVATE_USE, PG_U_SURROGATE, PG_U_DASH_PUNCTUATION, PG_U_OPEN_PUNCTUATION, PG_U_CLOSE_PUNCTUATION, PG_U_CONNECTOR_PUNCTUATION, PG_U_OTHER_PUNCTUATION, PG_U_MATH_SYMBOL, PG_U_CURRENCY_SYMBOL, PG_U_MODIFIER_SYMBOL, PG_U_OTHER_SYMBOL, PG_U_INITIAL_PUNCTUATION, PG_U_FINAL_PUNCTUATION)
- Called from (representative examples):
  - [icu_test](../i/icu_test.md) (in Unicode category tests for validation)

## Notes and Other Information
- Returns constant strings that match official Unicode general category names
- Includes an assertion to catch invalid category values during debugging
- Falls back to "Unrecognized" for invalid inputs in non-debug builds
- The string names use underscore separations (e.g., "Uppercase_Letter") following Unicode conventions
- Covers all major Unicode general categories as defined in the Unicode standard
- Used primarily for testing, debugging, and user interface display purposes

## Simplified Source

```c
const char *unicode_category_string(pg_unicode_category category) {
    // Map Unicode category enum to human-readable string
    switch (category) {
        case PG_U_UNASSIGNED:            return "Unassigned";
        case PG_U_UPPERCASE_LETTER:      return "Uppercase_Letter";
        case PG_U_LOWERCASE_LETTER:      return "Lowercase_Letter";
        case PG_U_TITLECASE_LETTER:      return "Titlecase_Letter";
        case PG_U_MODIFIER_LETTER:       return "Modifier_Letter";
        case PG_U_OTHER_LETTER:          return "Other_Letter";
        case PG_U_NONSPACING_MARK:       return "Nonspacing_Mark";
        case PG_U_ENCLOSING_MARK:        return "Enclosing_Mark";
        case PG_U_SPACING_MARK:          return "Spacing_Mark";
        case PG_U_DECIMAL_NUMBER:        return "Decimal_Number";
        case PG_U_LETTER_NUMBER:         return "Letter_Number";
        case PG_U_OTHER_NUMBER:          return "Other_Number";
        case PG_U_SPACE_SEPARATOR:       return "Space_Separator";
        case PG_U_LINE_SEPARATOR:        return "Line_Separator";
        case PG_U_PARAGRAPH_SEPARATOR:   return "Paragraph_Separator";
        case PG_U_CONTROL:               return "Control";
        case PG_U_FORMAT:                return "Format";
        case PG_U_PRIVATE_USE:           return "Private_Use";
        case PG_U_SURROGATE:             return "Surrogate";
        case PG_U_DASH_PUNCTUATION:      return "Dash_Punctuation";
        case PG_U_OPEN_PUNCTUATION:      return "Open_Punctuation";
        case PG_U_CLOSE_PUNCTUATION:     return "Close_Punctuation";
        case PG_U_CONNECTOR_PUNCTUATION: return "Connector_Punctuation";
        case PG_U_OTHER_PUNCTUATION:     return "Other_Punctuation";
        case PG_U_MATH_SYMBOL:           return "Math_Symbol";
        case PG_U_CURRENCY_SYMBOL:       return "Currency_Symbol";
        case PG_U_MODIFIER_SYMBOL:       return "Modifier_Symbol";
        case PG_U_OTHER_SYMBOL:          return "Other_Symbol";
        case PG_U_INITIAL_PUNCTUATION:   return "Initial_Punctuation";
        case PG_U_FINAL_PUNCTUATION:     return "Final_Punctuation";
    }

    // Should never reach here with valid input
    Assert(false);
    return "Unrecognized";
}
```