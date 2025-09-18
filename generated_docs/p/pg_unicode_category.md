# pg_unicode_category

## Location
[src/include/common/unicode_category.h:62-91](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/common/unicode_category.h#L62-L91)

## Overview
An enumerated type that defines all Unicode General Category values as specified by the Unicode standard, providing a way to classify Unicode characters into standardized categories.

## Definition


## Detailed Description
This enumeration defines the complete set of Unicode General Category values as standardized by the Unicode Consortium. Each value represents a specific character classification used throughout the Unicode standard. The enum serves as PostgreSQL's internal representation of these categories for Unicode character processing and classification operations.

The numeric values are specifically chosen to match the corresponding ICU (International Components for Unicode) UCharCategory values, ensuring compatibility with external Unicode libraries. The Unicode stability policy guarantees that no new general category values will be added, making this enumeration stable for long-term use.

## Parameters / Member Variables
-  (0): Unassigned characters (Cn category)
-  (1): Uppercase letters (Lu category)
-  (2): Lowercase letters (Ll category)
-  (3): Titlecase letters (Lt category)
-  (4): Modifier letters (Lm category)
-  (5): Other letters (Lo category)
-  (6): Nonspacing marks (Mn category)
-  (7): Enclosing marks (Me category)
-  (8): Spacing combining marks (Mc category)
-  (9): Decimal digits (Nd category)
-  (10): Letter-like numeric characters (Nl category)
-  (11): Other numeric characters (No category)
-  (12): Space characters (Zs category)
-  (13): Line separator characters (Zl category)
-  (14): Paragraph separator characters (Zp category)
-  (15): Control characters (Cc category)
-  (16): Format characters (Cf category)
-  (17): Private use characters (Co category)
-  (18): Surrogate code points (Cs category)
-  (19): Dash punctuation (Pd category)
-  (20): Open punctuation (Ps category)
-  (21): Close punctuation (Pe category)
-  (22): Connector punctuation (Pc category)
-  (23): Other punctuation (Po category)
-  (24): Math symbols (Sm category)
-  (25): Currency symbols (Sc category)
-  (26): Modifier symbols (Sk category)
-  (27): Other symbols (So category)
-  (28): Initial quotation marks (Pi category)
-  (29): Final quotation marks (Pf category)

## Dependencies
- Functions called/Symbols referenced:
  - None (enum definition)
- Called from (representative examples):
  - [unicode_category](../u/unicode_category.md)
  - [unicode_category_string](../u/unicode_category_string.md)
  - [unicode_category_abbrev](../u/unicode_category_abbrev.md)
  - [pg_u_isprint](pg_u_isprint.md)
  - [test_icu](../t/test_icu.md)

## Notes and Other Information
- The enum values are designed to match ICU UCharCategory for compatibility
- Unicode stability policy ensures this enumeration will never change
- Used extensively in PostgreSQL's Unicode character classification system
- Each enum value corresponds to a two-letter Unicode General Category abbreviation (shown in comments)
- Part of PostgreSQL's common Unicode processing infrastructure, available to both frontend and backend code
- Forms the foundation for bitmask operations in unicode_category.c for efficient multi-category comparisons