# pg_locale_t

## Location
[src/include/utils/pg_locale.h:99-141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/pg_locale.h#L99-L141)

## Overview
A pointer type that provides a handle to PostgreSQL's locale structure, serving as the primary interface for locale-aware string operations throughout the database system.

## Definition

```c
typedef struct pg_locale_struct *pg_locale_t;
```
## Detailed Description
The  type is PostgreSQL's primary locale handle, implemented as a pointer to . This design choice allows for efficient passing of locale information and enables null-pointer checks to determine if a locale is set. The type serves as the main interface for all locale-aware operations including string collation, transformation, and character conversion functions.

This typedef provides abstraction over the underlying locale implementation details, allowing code to work with locales without knowing whether they use builtin collations, system locale_t objects, or ICU collators.

## Parameters / Member Variables
- N/A (This is a typedef for a pointer type)

## Dependencies
- Functions called/Symbols referenced:
  - struct pg_locale_struct (the underlying structure)
- Called from (representative examples):
  - pg_strcoll: String collation comparison
  - pg_strxfrm: String transformation for sorting
  - pg_locale_deterministic: Check if locale is deterministic
  - pg_newlocale_from_collation: Create locale from collation OID
  - wchar2char/char2wchar: Character encoding conversion functions

## Notes and Other Information
- The pointer design allows for truth testing (NULL vs non-NULL) to determine if a locale is active
- Used extensively throughout PostgreSQL's text processing and sorting operations  
- A global  instance is available as the system default
- All locale-aware string functions accept this type as a parameter to specify locale behavior
- The type provides a clean abstraction that hides whether ICU, libc, or builtin collation is being used
- Located in src/include/utils/pg_locale.h:99