# locale_map

## Location
[src/port/win32setlocale.c:39-107](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/win32setlocale.c#L39-L107)

## Overview
A structure that defines mapping rules for transforming problematic locale names on Windows to their acceptable equivalents.

## Definition

```c
struct locale_map
{
	/*
	 * String in locale name to replace. Can be a single string (end is NULL),
	 * or separate start and end strings. If two strings are given, the locale
	 * name must contain both of them, and everything between them is
	 * replaced. This is used for a poor-man's regexp search, allowing
	 * replacement of "start.*end".
	 */
	const char *locale_name_start;
	const char *locale_name_end;

	const char *replacement;	/* string to replace the match with */
};
```
## Detailed Description
The  structure is used to define transformation rules for locale names on Windows. It supports two types of string replacement patterns:

1. **Simple replacement**: When  is NULL, the structure defines a simple string substitution where  is replaced with .

2. **Range replacement**: When both  and  are provided, the structure defines a "start.*end" pattern replacement where everything between (and including) the start and end strings is replaced with the  string.

This flexible design allows handling of various Windows locale naming issues, from simple country name substitutions to more complex pattern-based replacements.

## Parameters / Member Variables
- : The beginning of the string pattern to match and replace. For simple replacements, this is the entire string to replace.
- : The end of the string pattern for range replacements. Set to NULL for simple string replacements.
- : The string that will replace the matched pattern.

## Dependencies
- Functions called/Symbols referenced:
  - Used in static arrays  and 
- Called from:
  -  (receives arrays of this structure type)
  - Referenced in  and  static arrays

## Notes and Other Information
- Used exclusively in Windows locale handling to work around setlocale() bugs
- Two pre-defined arrays use this structure:
  - : Applied to input locale names before calling setlocale()
  - : Applied to output locale names returned by setlocale()
- The arrays are terminated with NULL entries (all fields set to NULL)
- Supports mapping problematic locale names like "Hong Kong S.A.R." to "HKG" and Norwegian locale names with non-ASCII characters to ASCII equivalents
- The range replacement feature implements a poor-man's regex functionality for "start.*end" pattern matching