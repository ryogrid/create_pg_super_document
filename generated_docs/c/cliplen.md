# cliplen

## Location
src/backend/utils/mb/mbutils.c: 1150 - 1160

## Overview
A simple string clipping function optimized for single-byte encodings that returns the effective length of a string up to a specified limit.

## Definition
```c
static int cliplen(const char *str, int len, int limit)
```

## Detailed Description
This is a static utility function that provides efficient string clipping for single-byte character encodings. It determines the effective length of a string by finding the minimum of the provided length, the specified limit, and the actual string length (stopping at null terminator). The function serves as an optimization case for multi-byte string clipping functions when dealing with single-byte encodings where each character is exactly one byte.

The function iterates through the string character by character until it reaches either the calculated limit or encounters a null terminator, whichever comes first.

## Parameters / Member Variables
- `str`: Pointer to the input string to be clipped
- `len`: The provided length of the input string in bytes
- `limit`: The maximum length allowed in the result

## Dependencies
- Functions called/Symbols referenced:
  - Min (macro for minimum value calculation)
- Called from (representative examples):
  - pgstat_clip_activity (multiple calls)
  - pg_encoding_mbcliplen
  - pg_mbcharcliplen

## Notes and Other Information
- This is a static function, only accessible within mbutils.c
- Optimized for single-byte encodings where character count equals byte count
- Used as a performance optimization by multi-byte clipping functions when they detect single-byte encoding
- Also used by pgstat_clip_activity for activity string clipping in the statistics collector
- The function handles null-terminated strings properly by stopping at the first null character