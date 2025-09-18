# str_numth

## Location
[src/backend/utils/adt/formatting.c:1561-1580](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L1561-L1580)

## Overview
A string manipulation function that converts a numeric string into its ordinal form by appending the appropriate ordinal suffix (ST/ND/RD/TH).

## Definition


## Detailed Description
The  function takes a numeric string and transforms it into its ordinal representation by appending the appropriate suffix. For example, "1" becomes "1st", "22" becomes "22nd", "103" becomes "103rd", etc. The function is designed to work efficiently by optionally copying the source number to a destination buffer (if they're different) and then appending the correct ordinal suffix determined by the  function.

This function is a key component in PostgreSQL's date/time formatting system, used when converting dates and times to human-readable ordinal formats (such as "1st of January" or "22nd day").

## Parameters / Member Variables
- : Destination buffer where the ordinal string will be stored (must have sufficient space for the original number plus suffix)
- : Source numeric string to be converted to ordinal form
- : Integer flag controlling the case of the suffix (0 for uppercase, 1 for lowercase)

## Dependencies
- Functions called/Symbols referenced:
  - strcpy (when dest != num)
  - strcat
  - [get_th](../g/get_th.md)
- Called from (representative examples):
  - [DCH_to_char](../D/DCH_to_char.md) (multiple locations for day/month formatting)
  - DCH_to_char_fsec
  - DCH_ZONED

## Notes and Other Information
- This is a static function only available within the formatting.c compilation unit  
- The function optimizes for the case where dest and num are the same buffer by skipping the strcpy operation
- The caller is responsible for ensuring the destination buffer has sufficient space for the original string plus the ordinal suffix (up to 2 additional characters)
- Commonly used in PostgreSQL's to_char() function for formatting dates with ordinal day/month representations
- The function assumes the input  is a valid numeric string - validation is handled by the underlying  function
- Return value is the destination buffer pointer, allowing for function chaining