# binaryCompareStrings

## Location
src/backend/utils/adt/jsonpath_exec.c: 3253 - 3273

## Overview
A utility function that performs byte-wise comparison of two strings without considering character encoding or locale.

## Definition
static int binaryCompareStrings(const char *s1, int len1, const char *s2, int len2)

## Detailed Description
The binaryCompareStrings function performs a straightforward per-byte comparison of two strings using memcmp for the overlapping portion, followed by length-based comparison if the byte contents are identical. This function provides a fast, encoding-agnostic comparison suitable for UTF-8 strings where byte order matches Unicode codepoint order. It returns standard comparison semantics: negative if s1 < s2, zero if equal, positive if s1 > s2.

## Parameters / Member Variables
- `s1`: Pointer to the first string to compare
- `len1`: Length of the first string in bytes
- `s2`: Pointer to the second string to compare
- `len2`: Length of the second string in bytes

## Dependencies
- Functions called/Symbols referenced:
  - memcmp (standard C library function)
  - Min macro
- Called from (representative examples):
  - [compareStrings](../c/compareStrings.md) (multiple call sites for UTF-8/ASCII and fallback scenarios)

## Notes and Other Information
This function is optimized for UTF-8 and ASCII encodings where byte-order comparison yields correct Unicode codepoint ordering. It's used both directly for UTF-8/ASCII strings and as a fallback for binary comparison when Unicode codepoint comparison results are equal but the original byte representations differ. The function handles strings that may not be null-terminated by relying on explicit length parameters.