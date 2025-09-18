# libpq_gettext

## Location
[src/interfaces/libpq/fe-misc.c:1329-1335](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L1329-L1335)

## Overview
Provides internationalized text lookup functionality for libpq by translating message IDs to localized strings.

## Definition
char *libpq_gettext(const char *msgid)

## Detailed Description
libpq_gettext is the main translation function for libpq's internationalization system. It ensures that the text domain is properly bound by calling libpq_binddomain(), then uses dgettext() to retrieve the translated version of the given message ID from the "libpq" text domain. This function is extensively used throughout libpq to provide localized error messages, status messages, and other user-visible text.

The function is a wrapper around the standard gettext dgettext() function, adding the necessary domain binding initialization and using the appropriate text domain for libpq messages.

## Parameters / Member Variables
- : The message identifier string to be translated (typically in English)

## Dependencies
- Functions called/Symbols referenced:
  - [libpq_binddomain](libpq_binddomain.md)
  - dgettext (gettext function)
  - PG_TEXTDOMAIN
- Called from (representative examples):
  - [pg_fe_scram_build_secret](../p/pg_fe_scram_build_secret.md)
  - [pg_GSS_continue](../p/pg_GSS_continue.md)
  - [auth_method_description](../a/auth_method_description.md)
  - [emitHostIdentityInfo](../e/emitHostIdentityInfo.md)
  - [PQerrorMessage](../P/PQerrorMessage.md)
  - [pqSetResultError](../p/pqSetResultError.md)
  - [libpq_append_error](libpq_append_error.md)

## Notes and Other Information
- This function is central to libpq's internationalization infrastructure
- Used extensively throughout libpq for translating user-facing messages
- Always calls libpq_binddomain() to ensure proper initialization
- Returns the translated string if available, or the original msgid if no translation exists
- Part of the standard gettext-based localization system used in PostgreSQL
- Critical for providing localized error messages and user interface text in client applications