# setup_locale_encoding

## Location
[src/bin/initdb/initdb.c:2663-2767](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/initdb/initdb.c#L2663-L2767)

## Overview
Configures and validates the locale settings and character encoding for the PostgreSQL database cluster during initialization, ensuring compatibility between locale and encoding choices.

## Definition
void setup_locale_encoding(void)

## Detailed Description
This function performs comprehensive locale and encoding setup during PostgreSQL database initialization. It handles multiple locale providers (libc, ICU, builtin), validates the compatibility between selected locales and character encodings, and provides detailed feedback about the resulting configuration.

The function automatically detects appropriate character encodings based on the system locale when no explicit encoding is specified. It implements platform-specific logic, particularly for Windows systems where UTF-8 is used as a fallback for incompatible encodings. The function also validates that the chosen encoding is suitable for server-side use and compatible with the selected locale provider.

Key responsibilities include setting up all locale categories (LC_COLLATE, LC_CTYPE, LC_MESSAGES, LC_MONETARY, LC_NUMERIC, LC_TIME), determining the appropriate database encoding, validating locale-encoding compatibility, and handling special requirements for different collation providers.

## Parameters / Member Variables
- Uses global variables for locale and encoding configuration:
  - `locale_provider`: The collation provider (LIBC, ICU, or BUILTIN)
  - `lc_*`: Various locale category settings
  - `datlocale`: Default collation locale
  - `encoding`: User-specified encoding (if any)
  - `encodingid`: Resolved encoding identifier

## Dependencies
- Functions called/Symbols referenced:
  - [setlocales](setlocales.md) (PostgreSQL locale setup function)
  - strcmp (C standard library)
  - printf (C standard library)
  - [collprovider_name](../c/collprovider_name.md) (PostgreSQL utility for provider names)
  - [pg_get_encoding_from_locale](../p/pg_get_encoding_from_locale.md) (PostgreSQL encoding detection)
  - [pg_valid_server_encoding_id](../p/pg_valid_server_encoding_id.md) (PostgreSQL encoding validation)
  - [pg_encoding_to_char](../p/pg_encoding_to_char.md) (PostgreSQL encoding utility)
  - [get_encoding_id](../g/get_encoding_id.md) (PostgreSQL encoding lookup)
  - [check_locale_encoding](../c/check_locale_encoding.md) (PostgreSQL locale-encoding validation)
  - [check_icu_locale_encoding](../c/check_icu_locale_encoding.md) (ICU-specific validation)
  - pg_log_error (PostgreSQL error logging)
  - pg_log_error_hint (PostgreSQL error hint logging)
  - pg_log_error_detail (PostgreSQL error detail logging)
  - [pg_fatal](../p/pg_fatal.md) (PostgreSQL fatal error function)
  - exit (C standard library)
- Constants referenced:
  - COLLPROVIDER_LIBC, COLLPROVIDER_ICU, COLLPROVIDER_BUILTIN
  - PG_SQL_ASCII, PG_UTF8
- Called from (representative examples):
  - [main](../m/main.md) (src/bin/initdb/initdb.c:3467)

## Notes and Other Information
- The function provides detailed output about the locale configuration, displaying either a simple message when all locale categories are identical or a comprehensive breakdown when they differ
- Automatic encoding detection prioritizes compatibility with the LC_CTYPE locale
- ICU provider requires UTF-8 encoding when SQL_ASCII would otherwise be selected
- Windows systems use UTF-8 as a fallback for server-incompatible encodings
- Builtin provider with "C.UTF-8" locale specifically requires UTF-8 encoding
- The function terminates the program if critical compatibility issues are detected
- Locale-encoding validation is performed for both LC_CTYPE and LC_COLLATE to ensure proper operation
- Error messages include helpful hints about using the -E option or selecting different locales

## Simplified Source

```c
void
setup_locale_encoding(void)
{
    // Set up and validate all locale categories
    setlocales();

    // Display locale configuration to user
    if (locale_provider == COLLPROVIDER_LIBC &&
        strcmp(lc_ctype, lc_collate) == 0 &&
        strcmp(lc_ctype, lc_time) == 0 &&
        strcmp(lc_ctype, lc_numeric) == 0 &&
        strcmp(lc_ctype, lc_monetary) == 0 &&
        strcmp(lc_ctype, lc_messages) == 0 &&
        (!datlocale || strcmp(lc_ctype, datlocale) == 0))
    {
        // All locales are the same - simple message
        printf(_("The database cluster will be initialized with locale \"%s\".\n"), lc_ctype);
    }
    else
    {
        // Different locales - detailed breakdown
        printf(_("The database cluster will be initialized with this locale configuration:\n"));
        printf(_("  locale provider:   %s\n"), collprovider_name(locale_provider));
        if (locale_provider != COLLPROVIDER_LIBC)
            printf(_("  default collation: %s\n"), datlocale);
        printf(_("  LC_COLLATE:  %s\n"
                 "  LC_CTYPE:    %s\n"
                 "  LC_MESSAGES: %s\n"
                 "  LC_MONETARY: %s\n"
                 "  LC_NUMERIC:  %s\n"
                 "  LC_TIME:     %s\n"),
               lc_collate, lc_ctype, lc_messages, lc_monetary, lc_numeric, lc_time);
    }

    // Determine character encoding
    if (!encoding) {
        // Auto-detect encoding from locale
        int ctype_enc = pg_get_encoding_from_locale(lc_ctype, true);

        // ICU doesn't support SQL_ASCII, use UTF-8 instead
        if (locale_provider == COLLPROVIDER_ICU && ctype_enc == PG_SQL_ASCII)
            ctype_enc = PG_UTF8;

        // Handle encoding detection failures
        if (ctype_enc == -1) {
            pg_log_error("could not find suitable encoding for locale \"%s\"", lc_ctype);
            pg_log_error_hint("Rerun %s with the -E option.", progname);
            pg_log_error_hint("Try \"%s --help\" for more information.", progname);
            exit(1);
        }
        else if (!pg_valid_server_encoding_id(ctype_enc)) {
            // Handle server-incompatible encodings
#ifdef WIN32
            // Windows: Fall back to UTF-8
            encodingid = PG_UTF8;
            printf(_("Encoding \"%s\" implied by locale is not allowed as a server-side encoding.\n"
                     "The default database encoding will be set to \"%s\" instead.\n"),
                   pg_encoding_to_char(ctype_enc), pg_encoding_to_char(encodingid));
#else
            // Other platforms: Report error
            pg_log_error("locale \"%s\" requires unsupported encoding \"%s\"",
                         lc_ctype, pg_encoding_to_char(ctype_enc));
            pg_log_error_detail("Encoding \"%s\" is not allowed as a server-side encoding.",
                                pg_encoding_to_char(ctype_enc));
            pg_log_error_hint("Rerun %s with a different locale selection.", progname);
            exit(1);
#endif
        }
        else {
            // Use detected encoding
            encodingid = ctype_enc;
            printf(_("The default database encoding has accordingly been set to \"%s\".\n"),
                   pg_encoding_to_char(encodingid));
        }
    }
    else {
        // Use user-specified encoding
        encodingid = get_encoding_id(encoding);
    }

    // Validate locale-encoding compatibility
    if (!check_locale_encoding(lc_ctype, encodingid) ||
        !check_locale_encoding(lc_collate, encodingid))
        exit(1);

    // Additional validation for specific providers
    if (locale_provider == COLLPROVIDER_BUILTIN) {
        if (strcmp(datlocale, "C.UTF-8") == 0 && encodingid != PG_UTF8)
            pg_fatal("builtin provider locale \"%s\" requires encoding \"%s\"",
                     datlocale, "UTF-8");
    }

    if (locale_provider == COLLPROVIDER_ICU &&
        !check_icu_locale_encoding(encodingid))
        exit(1);
}
```