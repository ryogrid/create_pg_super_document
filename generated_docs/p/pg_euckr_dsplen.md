# pg_euckr_dsplen

## Location
[src/common/wchar.c:222-231](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/wchar.c#L222-L231)

## Overview
A static function that determines the display width of a character sequence in EUC-KR (Extended Unix Code for Korean) encoding.

## Definition
```c
static int pg_euckr_dsplen(const unsigned char *s)
```

## Detailed Description
The pg_euckr_dsplen function calculates how many display columns a character sequence will occupy when rendered on screen in EUC-KR encoding. It delegates to the generic EUC display length function (pg_euc_dsplen), which handles the standard EUC display width rules. For most multi-byte Korean characters, this returns 2 (indicating they occupy 2 display columns), while ASCII characters return 1, and control characters return -1 or 0.

## Parameters / Member Variables
- `s`: Pointer to the first byte of a character sequence in EUC-KR encoding

## Dependencies
- Functions called/Symbols referenced:
  - [pg_euc_dsplen](pg_euc_dsplen.md)
- Called from (representative examples):
  - [pg_encoding_set_invalid](pg_encoding_set_invalid.md) (via function pointer assignment)

## Notes and Other Information
- This is a static function with internal linkage, only visible within the wchar.c compilation unit
- Display length differs from byte length: it indicates visual width rather than storage size
- Korean characters typically occupy 2 display columns (double-width) when rendered
- Control characters and null bytes may return special values (-1 for control characters, 0 for null)
- EUC-KR characters with SS2 or SS3 prefixes, as well as high-bit set characters, all display as 2-column width
- ASCII characters delegate to pg_ascii_dsplen for proper control character handling

## Simplified Source

```c
static int pg_euckr_dsplen(const unsigned char *s) {
    // Delegate to generic EUC display length function
    // Returns visual width in display columns:
    // - ASCII: 1 column (or -1/0 for control chars)
    // - Korean chars: 2 columns (double-width)
    return pg_euc_dsplen(s);
}
```