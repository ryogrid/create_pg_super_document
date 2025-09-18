# pg_strnxfrm_icu

## Location
src/backend/utils/adt/pg_locale.c: 2226 - 2272

## Overview
This static function transforms strings into ICU-based sort keys by converting to Unicode and using ICU collation services.

## Definition


## Detailed Description
pg_strnxfrm_icu provides ICU-based string transformation for generating sort keys that can be used for fast locale-aware string comparison. The function handles the complete pipeline from database encoding to ICU Unicode representation and finally to binary sort keys.

The transformation process involves:
1. Converting the input string from database encoding to Unicode (UChar)
2. Using ICU's ucol_getSortKey() to generate a binary sort key
3. Adjusting the result length to exclude the null terminator
4. Managing memory efficiently with stack/heap allocation strategy

This function is critical for PostgreSQL's support of ICU collations, enabling consistent and efficient string ordering according to Unicode Collation Algorithm (UCA) rules. The generated sort keys can be compared using simple byte comparison to achieve the same ordering as full ICU collation comparison.

## Parameters / Member Variables
- : Buffer to store the transformed binary sort key
- : Source string in database encoding
- : Length of source string (-1 indicates null-terminated)
- : Size of destination buffer
- LANG=C.UTF-8
LANGUAGE=
LC_CTYPE="C.UTF-8"
LC_NUMERIC="C.UTF-8"
LC_TIME="C.UTF-8"
LC_COLLATE="C.UTF-8"
LC_MONETARY="C.UTF-8"
LC_MESSAGES="C.UTF-8"
LC_PAPER="C.UTF-8"
LC_NAME="C.UTF-8"
LC_ADDRESS="C.UTF-8"
LC_TELEPHONE="C.UTF-8"
LC_MEASUREMENT="C.UTF-8"
LC_IDENTIFICATION="C.UTF-8"
LC_ALL=: ICU locale specification with collation rules

## Dependencies
- Functions called/Symbols referenced:
  - init_icu_converter
  - uchar_length
  - uchar_convert
  - ucol_getSortKey
  - palloc
  - pfree
  - TEXTBUFLEN
  - COLLPROVIDER_ICU
- Called from (representative examples):
  - pg_strxfrm (src/backend/utils/adt/pg_locale.c:2412)
  - pg_strnxfrm (src/backend/utils/adt/pg_locale.c:2449)

## Notes and Other Information
- This is a static function compiled only when ICU support is available
- Uses stack buffer optimization for small strings (TEXTBUFLEN threshold)
- The function decrements the result size to exclude ICU's automatic null terminator counting
- Includes assertions to verify ICU provider and result validity
- Handles both null-terminated (srclen = -1) and length-specified strings
- Memory management automatically handles cleanup for heap-allocated Unicode buffers
- The result is a binary sort key that maintains collation ordering through memcmp comparison
- Located in src/backend/utils/adt/pg_locale.c:2226-2272