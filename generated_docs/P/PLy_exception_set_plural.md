# PLy_exception_set_plural

## Location
src/pl/plpython/plpy_elog.c: 491 - 508

## Overview  
A specialized utility function that sets Python exception strings with printf-style formatting and proper plural form handling for internationalized messages.

## Definition
```c
void PLy_exception_set_plural(PyObject *exc, const char *fmt_singular, const char *fmt_plural, unsigned long n, ...)
```

## Detailed Description
This function extends the functionality of PLy_exception_set by providing proper plural form handling for internationalized error messages. It uses dngettext to select between singular and plural message formats based on the count value 'n', then applies printf-style formatting with variable arguments. This ensures that error messages are grammatically correct in different languages that have complex pluralization rules. The function is essential for providing localized, user-friendly error messages in PL/Python.

## Parameters / Member Variables
- `exc`: Python exception object to be set
- `fmt_singular`: Format string for singular form of the message
- `fmt_plural`: Format string for plural form of the message  
- `n`: Count value used to determine singular vs plural form
- `...`: Variable arguments for the format string

## Dependencies
- Functions called/Symbols referenced:
  - vsnprintf (standard C library function)
  - dngettext (internationalization function for plural forms)
  - PyErr_SetString (Python C API function)
  - TEXTDOMAIN (PostgreSQL text domain constant)
- Called from (representative examples):
  - PLy_cursor_plan
  - PLy_spi_execute_plan

## Notes and Other Information
- Uses dngettext for proper plural form selection based on locale rules
- Maintains the same 1024-byte buffer limitation as PLy_exception_set
- Critical for providing grammatically correct error messages in multiple languages
- Less commonly used than PLy_exception_set, but essential for count-dependent error messages
- Part of PostgreSQL's comprehensive internationalization support in PL/Python