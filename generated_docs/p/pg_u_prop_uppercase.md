# pg_u_prop_uppercase

## Location
[src/common/unicode_category.c:133-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_category.c#L133-L143)

## Overview
The pg_u_prop_uppercase function determines whether a given Unicode codepoint has the Uppercase property according to Unicode character classification standards.

## Definition
```c
bool pg_u_prop_uppercase(pg_wchar code)
```

## Detailed Description
This function checks if a Unicode character has the Uppercase property, which identifies characters that are uppercase letters or letter-like symbols. For ASCII characters (code < 0x80), it performs an optimized lookup using a bitmask operation on the unicode_opt_ascii table. For non-ASCII characters, it uses the range_search function to search through the unicode_uppercase array, which contains ranges of Unicode codepoints that have the Uppercase property.

The Uppercase property is distinct from the general category "Uppercase Letter" and includes additional characters beyond just traditional uppercase letters, encompassing various scripts and writing systems that have uppercase/lowercase distinctions.

## Parameters / Member Variables
- `code`: The Unicode codepoint (pg_wchar) to test for the Uppercase property

## Dependencies
- Functions called/Symbols referenced:
  - PG_U_PROP_UPPERCASE (constant bitmask for the Uppercase property)
  - [range_search](../r/range_search.md) (function for searching ranges)
  - lengthof (macro for array length)
- Called from (representative examples):
  - [pg_u_prop_cased](pg_u_prop_cased.md)
  - [pg_u_isupper](pg_u_isupper.md)
  - [icu_test](../i/icu_test.md) (testing function)

## Notes and Other Information
- Optimized for ASCII characters with direct bitmask lookup
- Essential for case-sensitive text operations and collation
- Used in implementing proper uppercase detection across all Unicode scripts
- Part of the casing system that supports PostgreSQL's text processing and pattern matching
- Works in conjunction with pg_u_prop_lowercase for comprehensive case handling
- Located in src/common/unicode_category.c:133-143

## Simplified Source

```c
bool
pg_u_prop_uppercase(pg_wchar code)
{
    // Fast path for ASCII characters using bitmask lookup
    if (code < 0x80)
        return unicode_opt_ascii[code].properties & PG_U_PROP_UPPERCASE;

    // For non-ASCII characters, search the uppercase ranges table
    return range_search(unicode_uppercase,
                        lengthof(unicode_uppercase),
                        code);
}
```