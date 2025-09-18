# pg_u_prop_cased

## Location
[src/common/unicode_category.c:144-158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_category.c#L144-L158)

## Overview
The pg_u_prop_cased function determines whether a given Unicode codepoint has the Cased property, identifying characters that participate in case distinctions (uppercase/lowercase).

## Definition
```c
bool pg_u_prop_cased(pg_wchar code)
```

## Detailed Description
This function checks if a Unicode character has the Cased property, which identifies characters that have case (upper/lower/title case) or case-related properties. For ASCII characters (code < 0x80), it performs an optimized lookup using a bitmask operation on the unicode_opt_ascii table.

For non-ASCII characters, it uses a comprehensive approach that checks multiple conditions:
1. Gets the character's general category and checks if it's a titlecase letter (PG_U_LT_MASK)
2. Checks if the character has the Lowercase property using pg_u_prop_lowercase()
3. Checks if the character has the Uppercase property using pg_u_prop_uppercase()

A character is considered cased if any of these conditions are true, making this a union of all case-related properties.

## Parameters / Member Variables
- `code`: The Unicode codepoint (pg_wchar) to test for the Cased property

## Dependencies
- Functions called/Symbols referenced:
  - PG_U_PROP_CASED (constant bitmask for the Cased property)
  - PG_U_CATEGORY_MASK (macro for category masking)
  - [unicode_category](../u/unicode_category.md) (function to get character category)
  - PG_U_LT_MASK (mask for titlecase letters)
  - [pg_u_prop_lowercase](pg_u_prop_lowercase.md) (function to check lowercase property)
  - [pg_u_prop_uppercase](pg_u_prop_uppercase.md) (function to check uppercase property)
- Called from (representative examples):
  - [icu_test](../i/icu_test.md) (testing function)

## Notes and Other Information
- Comprehensive approach combining category checks and property checks
- Essential for case folding, case conversion, and case-sensitive operations
- More inclusive than individual case property checks as it covers all case variants
- Optimized for ASCII with direct property lookup
- Critical for implementing Unicode-compliant text processing in PostgreSQL
- Located in src/common/unicode_category.c:144-158