# setlocale_perl

## Location
[src/pl/plperl/plperl.c:4181-4247](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plperl/plperl.c#L4181-L4247)

## Overview
A static function that wraps the standard setlocale() function to properly notify Perl about locale changes in PostgreSQL's PL/Perl environment.

## Definition
```c
static char *setlocale_perl(int category, char *locale)
```

## Detailed Description
The `setlocale_perl` function is a wrapper around the standard C library `setlocale()` function that ensures Perl's internal locale handling is properly synchronized when PostgreSQL changes locale settings. When a locale change is requested, this function first calls the standard `setlocale()` to perform the actual locale change, then notifies Perl about the change by calling appropriate Perl locale update functions (`new_ctype`, `new_collate`, `new_numeric`) depending on which locale category was modified.

This synchronization is critical because Perl maintains its own internal locale state, and if PostgreSQL changes the system locale without notifying Perl, it can lead to inconsistencies in string operations, character classification, and numeric formatting within PL/Perl code.

## Parameters / Member Variables
- `category`: The locale category to set (LC_CTYPE, LC_COLLATE, LC_NUMERIC, LC_ALL, etc.)
- `locale`: The locale string to set (e.g., "C", "en_US.UTF-8", NULL to query current setting)

## Dependencies
- Functions called/Symbols referenced:
  - dTHX (Perl threading context macro)
  - setlocale (standard C library function, called multiple times)
  - new_ctype (Perl locale update function, conditionally compiled)
  - new_collate (Perl locale update function, conditionally compiled)  
  - new_numeric (Perl locale update function, conditionally compiled)
- Called from (representative examples):
  - PLPERL_RESTORE_LOCALE (macro)

## Notes and Other Information
- This is a static function with internal linkage, only visible within plperl.c
- Uses conditional compilation based on locale support macros (USE_LOCALE_CTYPE, USE_LOCALE_COLLATE, USE_LOCALE_NUMERIC)
- Handles LC_ALL category specially by querying individual locale categories after setting
- The function returns the same value as the underlying setlocale() call
- Critical for maintaining consistency between PostgreSQL's locale settings and Perl's internal locale state
- Located in src/pl/plperl/plperl.c at lines 4181-4247
- Part of the PL/Perl procedural language extension's locale management system

## Simplified Source
```c
static char *setlocale_perl(int category, char *locale) {
    dTHX;
    char *RETVAL = setlocale(category, locale);

    if (RETVAL) {
        // Update Perl's locale state for each relevant category

#ifdef USE_LOCALE_CTYPE
        if (category == LC_CTYPE || category == LC_ALL) {
            char *newctype = (category == LC_ALL) ? setlocale(LC_CTYPE, NULL) : RETVAL;
            new_ctype(newctype);
        }
#endif

#ifdef USE_LOCALE_COLLATE
        if (category == LC_COLLATE || category == LC_ALL) {
            char *newcoll = (category == LC_ALL) ? setlocale(LC_COLLATE, NULL) : RETVAL;
            new_collate(newcoll);
        }
#endif

#ifdef USE_LOCALE_NUMERIC
        if (category == LC_NUMERIC || category == LC_ALL) {
            char *newnum = (category == LC_ALL) ? setlocale(LC_NUMERIC, NULL) : RETVAL;
            new_numeric(newnum);
        }
#endif
    }

    return RETVAL;
}
```