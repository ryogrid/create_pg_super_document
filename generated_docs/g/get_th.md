# get_th

## Location
[src/backend/utils/adt/formatting.c:1516-1560](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/formatting.c#L1516-L1560)

## Overview
A utility function that returns the appropriate ordinal suffix (ST/ND/RD/TH) for numbers, supporting both uppercase and lowercase variants based on English ordinal number rules.

## Definition

```c
typedef int32_t (*ICU_Convert_Func) (UChar *dest, int32_t destCapacity,
									 const UChar *src, int32_t srcLength,
									 const char *locale,
									 UErrorCode *pErrorCode);
```
## Detailed Description
The  function determines the correct ordinal suffix for a given number string according to English grammar rules. It analyzes the last digit(s) of the number to determine whether to return "ST", "ND", "RD", or "TH" (or their lowercase equivalents). The function implements the standard English ordinal rules:

- Numbers ending in 1 get "ST" (except for 11, which gets "TH")
- Numbers ending in 2 get "ND" (except for 12, which gets "TH") 
- Numbers ending in 3 get "RD" (except for 13, which gets "TH")
- All other numbers (including teens 11-19) get "TH"

The function validates that the input string represents a valid number and handles the special case of "teen" numbers (10-19) which all receive the "TH" suffix regardless of their final digit.

## Parameters / Member Variables
- : A null-terminated string representing the number for which to determine the ordinal suffix
- : An integer flag controlling case output (0/TH_UPPER for uppercase, 1 for lowercase)

## Dependencies
- Functions called/Symbols referenced:
  - strlen
  - isdigit
  - ereport
  - ERROR
  - [errcode](../e/errcode.md)
  - ERRCODE_INVALID_TEXT_REPRESENTATION
  - [errmsg](../e/errmsg.md)
  - TH_UPPER
  - numTH (array containing uppercase suffixes)
  - numth (array containing lowercase suffixes)
- Called from (representative examples):
  - [str_numth](../s/str_numth.md)
  - [NUM_processor](../N/NUM_processor.md)
  - DCH_ZONED

## Notes and Other Information
- This is a static function only available within the formatting.c compilation unit
- The function performs input validation and will raise an ERROR if the input string doesn't end with a digit
- The special handling of "teen" numbers (11-19) is implemented by checking if the second-to-last digit is '1'
- The function relies on pre-defined arrays numTH and numth containing the actual suffix strings
- Used extensively in PostgreSQL's date/time and numeric formatting functions for generating ordinal representations
- The function is designed to be efficient for repeated calls during formatting operations

## Simplified Source

```c
static const char *get_th(char *num, int type) {
    int len = strlen(num);
    int last = num[len - 1];

    // Validate input is a number
    if (!isdigit(last)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
                       errmsg("\"%s\" is not a number", num)));
    }

    // Handle teen numbers (11-19): all get "TH"
    if ((len > 1) && (num[len - 2] == '1')) {
        last = 0;  // Force "TH" suffix
    }

    // Determine ordinal suffix based on last digit
    switch (last) {
        case '1':
            return (type == TH_UPPER) ? numTH[0] : numth[0];  // "ST"/"st"
        case '2':
            return (type == TH_UPPER) ? numTH[1] : numth[1];  // "ND"/"nd"
        case '3':
            return (type == TH_UPPER) ? numTH[2] : numth[2];  // "RD"/"rd"
        default:
            return (type == TH_UPPER) ? numTH[3] : numth[3];  // "TH"/"th"
    }
}
```