# unicode_category_abbrev

## Location
[src/common/unicode_category.c:406-480](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_category.c#L406-L480)

## Overview
Converts a Unicode general category enumeration value into its corresponding two-character abbreviation code as defined by the Unicode standard.

## Definition

```c
const char *
unicode_category_abbrev(pg_unicode_category category)
```
## Detailed Description
This function provides the official Unicode two-character abbreviation for Unicode general category values. Each Unicode character belongs to one of these standardized categories, and the abbreviations follow the Unicode consortium's naming conventions. The first character represents the major category (L for Letter, N for Number, P for Punctuation, etc.) and the second character represents the subcategory (u for uppercase, l for lowercase, d for decimal, etc.). This function is commonly used in text processing, character classification, and Unicode-compliant operations where compact category representation is needed.

## Parameters / Member Variables
- `category`: The pg_unicode_category enumeration value to convert to its two-character abbreviation
## Dependencies
- Functions called/Symbols referenced:
  - [pg_unicode_category](../p/pg_unicode_category.md) (parameter type)
  - All PG_U_* category constants (PG_U_UNASSIGNED, PG_U_UPPERCASE_LETTER, PG_U_LOWERCASE_LETTER, PG_U_TITLECASE_LETTER, PG_U_MODIFIER_LETTER, PG_U_OTHER_LETTER, PG_U_NONSPACING_MARK, PG_U_ENCLOSING_MARK, PG_U_SPACING_MARK, PG_U_DECIMAL_NUMBER, PG_U_LETTER_NUMBER, PG_U_OTHER_NUMBER, PG_U_SPACE_SEPARATOR, PG_U_LINE_SEPARATOR, PG_U_PARAGRAPH_SEPARATOR, PG_U_CONTROL, PG_U_FORMAT, PG_U_PRIVATE_USE, PG_U_SURROGATE, PG_U_DASH_PUNCTUATION, PG_U_OPEN_PUNCTUATION, PG_U_CLOSE_PUNCTUATION, PG_U_CONNECTOR_PUNCTUATION, PG_U_OTHER_PUNCTUATION, PG_U_MATH_SYMBOL, PG_U_CURRENCY_SYMBOL, PG_U_MODIFIER_SYMBOL, PG_U_OTHER_SYMBOL, PG_U_INITIAL_PUNCTUATION, PG_U_FINAL_PUNCTUATION)
- Called from (representative examples):
  - [icu_test](../i/icu_test.md) (in Unicode category tests for validation)

## Notes and Other Information
- Returns standard Unicode two-character category codes (e.g., "Lu" for Uppercase Letter, "Nd" for Decimal Number)
- The abbreviation scheme follows Unicode standard conventions: first character for major category, second for subcategory
- Major categories include: L (Letter), M (Mark), N (Number), P (Punctuation), S (Symbol), Z (Separator), C (Other)
- Includes an assertion to catch invalid category values during debugging
- Falls back to "??" for invalid inputs in non-debug builds
- Used in text processing, regular expressions, and Unicode compliance operations
- Provides a compact representation suitable for logging, debugging, and algorithmic processing

## Simplified Source

```c
const char *unicode_category_abbrev(pg_unicode_category category) {
    // Map Unicode category enum to two-character abbreviation
    switch (category) {
        case PG_U_UNASSIGNED:            return "Cn";
        case PG_U_UPPERCASE_LETTER:      return "Lu";
        case PG_U_LOWERCASE_LETTER:      return "Ll";
        case PG_U_TITLECASE_LETTER:      return "Lt";
        case PG_U_MODIFIER_LETTER:       return "Lm";
        case PG_U_OTHER_LETTER:          return "Lo";
        case PG_U_NONSPACING_MARK:       return "Mn";
        case PG_U_ENCLOSING_MARK:        return "Me";
        case PG_U_SPACING_MARK:          return "Mc";
        case PG_U_DECIMAL_NUMBER:        return "Nd";
        case PG_U_LETTER_NUMBER:         return "Nl";
        case PG_U_OTHER_NUMBER:          return "No";
        case PG_U_SPACE_SEPARATOR:       return "Zs";
        case PG_U_LINE_SEPARATOR:        return "Zl";
        case PG_U_PARAGRAPH_SEPARATOR:   return "Zp";
        case PG_U_CONTROL:               return "Cc";
        case PG_U_FORMAT:                return "Cf";
        case PG_U_PRIVATE_USE:           return "Co";
        case PG_U_SURROGATE:             return "Cs";
        case PG_U_DASH_PUNCTUATION:      return "Pd";
        case PG_U_OPEN_PUNCTUATION:      return "Ps";
        case PG_U_CLOSE_PUNCTUATION:     return "Pe";
        case PG_U_CONNECTOR_PUNCTUATION: return "Pc";
        case PG_U_OTHER_PUNCTUATION:     return "Po";
        case PG_U_MATH_SYMBOL:           return "Sm";
        case PG_U_CURRENCY_SYMBOL:       return "Sc";
        case PG_U_MODIFIER_SYMBOL:       return "Sk";
        case PG_U_OTHER_SYMBOL:          return "So";
        case PG_U_INITIAL_PUNCTUATION:   return "Pi";
        case PG_U_FINAL_PUNCTUATION:     return "Pf";
    }

    // Should never reach here with valid input
    Assert(false);
    return "??";
}
```