# clean_encoding_name

## Location
[src/common/encnames.c:524-548](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/encnames.c#L524-L548)

## Overview
A static utility function that normalizes encoding names by removing non-alphanumeric characters and converting uppercase letters to lowercase.

## Definition

```c
static char *
clean_encoding_name(const char *key, char *newkey)
```
## Detailed Description
The  function processes an encoding name string to create a normalized version suitable for comparison and lookup operations. It removes all non-alphanumeric characters (such as hyphens, underscores, spaces) and converts uppercase letters to lowercase. This normalization is essential for encoding name matching since encoding names can be written in various formats (e.g., "UTF-8", "utf8", "UTF_8") but should all be treated as equivalent.

The function iterates through each character of the input string, filters out irrelevant characters, performs case conversion, and stores the result in the provided buffer. This approach ensures consistent encoding name representation across the PostgreSQL system.

## Parameters / Member Variables
- : The original encoding name string to be normalized
- : Output buffer where the cleaned encoding name will be stored (caller must ensure sufficient buffer size)

## Dependencies
- Functions called/Symbols referenced:
  - isalnum (standard C library function for checking alphanumeric characters)
- Called from (representative examples):
  - [pg_char_to_encoding](../p/pg_char_to_encoding.md)

## Notes and Other Information
- This is a static function, only accessible within the encnames.c file
- The caller is responsible for providing a sufficiently large buffer for the output
- The function assumes the input string is null-terminated
- Case conversion is performed using simple ASCII arithmetic (uppercase A-Z to lowercase a-z)
- All non-alphanumeric characters are stripped from the encoding name