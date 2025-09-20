# num_word

## Location
[src/backend/utils/adt/cash.c:39-90](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L39-L90)

## Overview
A private utility function that converts a Cash value (integer) to its English word representation for amounts up to 999.

## Definition

```c
static const char *
num_word(Cash value)
```
## Detailed Description
The  function converts integer values from 0 to 999 into their corresponding English word representations. It handles various cases including single digits, teens, tens, and hundreds with appropriate grammatical formatting. The function uses static arrays for efficient word lookup and a static buffer for constructing compound number words. It's specifically designed to support the cash_words function for converting monetary amounts to textual format.

The function handles several formatting cases:
- Numbers 0-20: Direct lookup from the "small" array
- Multiples of 100: "X hundred" format  
- Numbers 21-99: Combines tens and units ("twenty one", "thirty", etc.)
- Numbers 100-999: Combines hundreds with tens/units using "and" for teens

## Parameters / Member Variables
- : Cash value (integer) to convert to words, expected to be in range 0-999

## Dependencies
- Functions called/Symbols referenced:
  - Cash (type)
  - sprintf (standard library function)
- Called from (representative examples):
  - [cash_words](../c/cash_words.md)

## Notes and Other Information
- Uses static storage for both the word arrays and output buffer, making it non-reentrant
- The "small" array contains words for 0-27, with entries 21-27 representing multiples of ten
- The "big" pointer provides convenient access to the tens words starting from "twenty"
- Limited to values 0-999; behavior with larger values is undefined
- Part of PostgreSQL's cash data type implementation for monetary value formatting