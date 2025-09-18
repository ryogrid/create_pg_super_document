# rfmtlong

## Location
[src/interfaces/ecpg/compatlib/informix.c:768-961](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/compatlib/informix.c#L768-L961)

## Overview
A complex numeric formatting function that converts a long integer to a formatted ASCII string using Informix-style format specifiers, supporting advanced formatting features like alignment, padding, sign handling, and bracket notation.

## Definition


## Detailed Description
The `rfmtlong` function is part of PostgreSQL's ECPG Informix compatibility layer that provides sophisticated numeric formatting capabilities. It takes a long integer value and formats it according to a complex format string that supports various Informix-style formatting specifiers.

The function supports multiple formatting features:
- **Alignment**: Left alignment with '<' specifier
- **Padding**: Various padding characters ('*', '&', '#')
- **Sign handling**: Explicit sign display with '+' and '-' specifiers
- **Bracket notation**: Negative number representation with '(' and ')'
- **Currency symbols**: Dollar sign formatting with '$'
- **Decimal positioning**: Precise decimal point placement with '.'
- **Comma separation**: Thousands separator with ','

The formatting process involves:
1. Initializing the value structure with `initValue`
2. Analyzing the format string for special characters
3. Determining decimal point position with `getRightMostDot`
4. Processing format characters from right to left
5. Building the output string and reversing it for correct order

## Parameters / Member Variables
- `lng_val`: The long integer value to be formatted
- `fmt`: Format string containing Informix-style formatting specifiers
- `outbuf`: Output buffer where the formatted string will be stored

## Dependencies
- Functions called/Symbols referenced:
  - malloc (allocates temporary formatting buffer)
  - [initValue](../i/initValue.md) (initializes the value structure with digit information)
  - [getRightMostDot](../g/getRightMostDot.md) (finds decimal point position in format string)
  - strchr (searches for specific format characters)
  - strlen (calculates string lengths)
  - strcat (builds the formatted output)
  - free (deallocates temporary buffers and value string)
- Called from (representative examples):
  - Available through ECPG_INFORMIX_EXTRA_CHARS interface
  - Used in test cases (fmtlong in compat_informix-rfmtlong.c)

## Notes and Other Information
- Returns 0 on success, -1 on memory allocation failure
- Part of the Informix compatibility library (`compatlib/informix.c`)
- Supports complex format strings with multiple specifiers that can be combined
- Uses a reverse-parsing approach, processing format string from right to left
- Handles memory management for both temporary buffers and value structure
- The output string is built in reverse and then reversed again for correct order
- Format specifiers include: '<', '()', '+', '-', '*', '&', '#', '$', ',', '.'
- Sets errno to ENOMEM on memory allocation failures
- Assumes output buffer is sufficiently sized for the formatted result