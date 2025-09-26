# CaseKind

## Location
[src/include/common/unicode_case_table.h:29-34](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/unicode_case_table.h#L29-L34)

## Overview
CaseKind is an enumeration that defines the three primary Unicode case transformation types used in PostgreSQL string case conversion operations.

## Definition
```c
typedef enum
{
    CaseLower = 0,
    CaseTitle = 1, 
    CaseUpper = 2,
    NCaseKind
} CaseKind;
```

## Detailed Description
CaseKind is a fundamental enumeration type used throughout PostgreSQL Unicode case handling system. It defines the three standard case transformation modes supported by the Unicode standard:

- **CaseLower (0)**: Represents lowercase transformation, converting characters to their lowercase equivalents
- **CaseTitle (1)**: Represents title case transformation, which capitalizes the first character of words and lowercases the rest
- **CaseUpper (2)**: Represents uppercase transformation, converting characters to their uppercase equivalents
- **NCaseKind**: A sentinel value representing the total number of case kinds, used for array sizing and bounds checking

This enumeration serves as an index into case mapping arrays and as a parameter to case conversion functions. The design allows for efficient array-based lookups in the Unicode case mapping tables, where each Unicode codepoint can have up to three different case mappings stored in a `simplemap[NCaseKind]` array.

## Parameters / Member Variables
- `CaseLower`: Enumeration value (0) for lowercase transformation
- `CaseTitle`: Enumeration value (1) for title case transformation  
- `CaseUpper`: Enumeration value (2) for uppercase transformation
- `NCaseKind`: Sentinel value representing the count of case transformation types

## Dependencies
- Functions called/Symbols referenced:
  - NCaseKind (used as array size specifier)
- Called from (representative examples):
  - [convert_case](../c/convert_case.md) (uses CaseKind as parameter and for character-level case determination)
  - Used in pg_case_map struct definition for simplemap array indexing

## Notes and Other Information
- The enum values are explicitly assigned (0, 1, 2) to ensure consistent array indexing across the codebase
- NCaseKind serves a dual purpose as both an enum member and a compile-time constant for array sizing
- This enumeration is part of PostgreSQL Unicode case handling infrastructure located in src/include/common/unicode_case_table.h:23-29
- The three case types correspond to the standard Unicode case transformation categories
- Used extensively in the case mapping table where each Unicode codepoint has an array of transformations indexed by CaseKind values
- Title case transformation requires special word boundary detection logic, as implemented in the convert_case function