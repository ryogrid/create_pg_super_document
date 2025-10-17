# pg_bindtextdomain

## Location
[src/backend/utils/init/miscinit.c:1935-1947](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/init/miscinit.c#L1935-L1947)

## Overview
Initializes the gettext message catalog binding for internationalization (i18n) support in PostgreSQL by setting up the correct locale directory path and codeset encoding.

## Definition

```c
void
pg_bindtextdomain(const char *domain)
```
## Detailed Description
The `pg_bindtextdomain` function is responsible for setting up internationalization support by binding a gettext message domain to the appropriate locale directory and codeset. This function is essential for PostgreSQL's localization framework, enabling the system to display error messages and other text in the user's preferred language.

The function operates only when compiled with NLS (Native Language Support) enabled (`ENABLE_NLS`). It performs two critical operations:
1. Determines the correct locale directory path based on the PostgreSQL executable location
2. Binds the specified text domain to both the locale directory and the appropriate character encoding

This function is typically called during PostgreSQL initialization phases and by various procedural language extensions (_PG_init functions) to ensure proper localization support.

## Parameters / Member Variables
- `domain`: The gettext message domain name (e.g., "postgres", "plpgsql", "plperl") to be bound to the locale directory and codeset

## Dependencies
- Functions called/Symbols referenced:
  - [get_locale_path](../g/get_locale_path.md): Constructs the locale directory path based on the executable location
  - `bindtextdomain`: Standard gettext function that binds a domain to a directory
  - [pg_bind_textdomain_codeset](pg_bind_textdomain_codeset.md): Sets the appropriate character encoding for the text domain
- Called from (representative examples):
  - [_PG_init](../P/_PG_init.md) (in plperl.c): PL/Perl language initialization  
  - [_PG_init](../P/_PG_init.md) (in plpy_main.c): PL/Python language initialization
  - [_PG_init](../P/_PG_init.md) (in pltcl.c): PL/Tcl language initialization
  - `INIT_PG_OVERRIDE_ROLE_LOGIN`: Role login override macro

## Notes and Other Information
- The function is conditionally compiled and only active when `ENABLE_NLS` is defined
- It depends on the global variable `my_exec_path` being properly initialized with the PostgreSQL executable path
- The locale path is constructed relative to the PostgreSQL installation directory using the `LOCALEDIR` and `PGBINDIR` constants
- Character encoding binding is crucial for proper display of localized messages, especially in multi-byte character environments
- This function must be called early in the initialization process before most error reporting mechanisms are available
- The function is used by procedural language extensions to ensure their error messages and documentation can be properly localized

## Simplified Source

```c
void pg_bindtextdomain(const char *domain)
{
#ifdef ENABLE_NLS
    // Only proceed if executable path is known
    if (my_exec_path[0] != '\0') {
        char locale_path[MAXPGPATH];

        // Get locale directory path relative to executable
        get_locale_path(my_exec_path, locale_path);

        // Bind text domain to locale directory
        bindtextdomain(domain, locale_path);

        // Set appropriate character encoding for domain
        pg_bind_textdomain_codeset(domain);
    }
#endif
}
```