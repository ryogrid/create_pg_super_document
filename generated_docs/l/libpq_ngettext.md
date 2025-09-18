# libpq_ngettext

## Location
[src/interfaces/libpq/fe-misc.c:1336-1350](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-misc.c#L1336-L1350)

## Overview
Provides pluralized internationalized text lookup functionality for libpq by selecting appropriate singular or plural message forms based on a count value.

## Definition
char *libpq_ngettext(const char *msgid, const char *msgid_plural, unsigned long n)

## Detailed Description
libpq_ngettext handles pluralization in libpq's internationalization system. It ensures proper text domain binding by calling libpq_binddomain(), then uses dngettext() to retrieve either the singular or plural form of a message based on the count value 'n'. This function is essential for generating grammatically correct messages in different languages, as pluralization rules vary significantly across languages.

The function follows the standard gettext pluralization mechanism, where the appropriate message form is selected according to the target language's plural rules defined in the message catalog.

## Parameters / Member Variables
- : The singular form message identifier string (typically in English)
- : The plural form message identifier string (typically in English)  
- : The count value that determines which plural form to use

## Dependencies
- Functions called/Symbols referenced:
  - [libpq_binddomain](libpq_binddomain.md)
  - dngettext (gettext function)
  - PG_TEXTDOMAIN
- Called from (representative examples):
  - [pqGetNegotiateProtocolVersion3](../p/pqGetNegotiateProtocolVersion3.md)
  - [pq_verify_peer_name_matches_certificate](../p/pq_verify_peer_name_matches_certificate.md)

## Notes and Other Information
- Essential for proper pluralization in internationalized messages
- Used less frequently than libpq_gettext but critical for count-dependent messages
- Supports complex pluralization rules that vary by language (some languages have multiple plural forms)
- Always calls libpq_binddomain() to ensure proper initialization
- Returns the appropriate singular or plural form based on the language's pluralization rules
- Part of the standard gettext-based localization system used in PostgreSQL