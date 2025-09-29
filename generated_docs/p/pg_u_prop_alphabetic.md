# pg_u_prop_alphabetic

## Location
[src/common/unicode_category.c:111-121](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/unicode_category.c#L111-L121)

## Overview
The pg_u_prop_alphabetic function determines whether a given Unicode codepoint has the Alphabetic property according to Unicode character classification standards.

## Definition
```c
bool pg_u_prop_alphabetic(pg_wchar code)
```

## Detailed Description
This function checks if a Unicode character has the Alphabetic property, which includes letters and letter-like characters. For ASCII characters (code < 0x80), it performs a fast lookup using a bitmask operation on the unicode_opt_ascii table. For non-ASCII characters, it uses the range_search function to search through the unicode_alphabetic array, which contains ranges of Unicode codepoints that have the Alphabetic property.

The Alphabetic property is a fundamental Unicode property that encompasses not just traditional letters but also ideographs, syllables, and other characters used in writing systems that function as letters.

## Parameters / Member Variables
- `code`: The Unicode codepoint (pg_wchar) to test for the Alphabetic property

## Dependencies
- Functions called/Symbols referenced:
  - PG_U_PROP_ALPHABETIC (constant bitmask for the Alphabetic property)
  - [range_search](../r/range_search.md) (function for searching ranges)
  - lengthof (macro for array length)
- Called from (representative examples):
  - [pg_u_isalpha](pg_u_isalpha.md)
  - [icu_test](../i/icu_test.md) (testing function)

## Notes and Other Information
- Optimized for ASCII characters with direct bitmask lookup
- Uses range search for efficient non-ASCII character property checking
- Part of the Unicode property system that supports PostgreSQL's text processing capabilities
- Essential for implementing proper alphabetic character detection across all Unicode scripts
- Located in src/common/unicode_category.c:111-121

## Simplified Source

```c
bool
pg_u_prop_alphabetic(pg_wchar code)
{
    // Fast path for ASCII characters using bitmask lookup
    if (code < 0x80)
        return unicode_opt_ascii[code].properties & PG_U_PROP_ALPHABETIC;

    // For non-ASCII characters, search the alphabetic ranges table
    return range_search(unicode_alphabetic,
                        lengthof(unicode_alphabetic),
                        code);
}
```