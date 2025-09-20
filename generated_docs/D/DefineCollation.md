# DefineCollation

## Location
[src/backend/commands/collationcmds.c:53-399](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/collationcmds.c#L53-L399)

## Overview
DefineCollation implements the CREATE COLLATION SQL command, creating a new collation object in the PostgreSQL system catalog with specified locale and provider settings.

## Definition

```c
ObjectAddress
DefineCollation(ParseState *pstate, List *names, List *parameters, bool if_not_exists)
```
## Detailed Description
This function processes the CREATE COLLATION command by:
1. Parsing and validating collation parameters (locale, provider, rules, etc.)
2. Handling two creation modes: FROM existing collation or with explicit parameters
3. Supporting multiple collation providers: libc, ICU, and builtin
4. Performing validation based on the selected provider
5. Creating the collation entry in the system catalog
6. Testing that the locale can be loaded

The function supports IF NOT EXISTS semantics and handles complex parameter combinations with appropriate error checking for conflicting or missing options.

## Parameters / Member Variables
- : ParseState for error reporting with location information
- : List of qualified names specifying the collation name and schema
- : List of DefElem structures containing collation options
- : Boolean flag for IF NOT EXISTS behavior

## Dependencies
- Functions called/Symbols referenced:
  - [QualifiedNameGetCreationNamespace](../Q/QualifiedNameGetCreationNamespace.md)
  - [object_aclcheck](../o/object_aclcheck.md)
  - [get_collation_oid](../g/get_collation_oid.md)
  - [builtin_validate_locale](../b/builtin_validate_locale.md)
  - [icu_language_tag](../i/icu_language_tag.md)
  - [icu_validate_locale](../i/icu_validate_locale.md)
  - [get_collation_actual_version](../g/get_collation_actual_version.md)
  - [CollationCreate](../C/CollationCreate.md)
  - [pg_newlocale_from_collation](../p/pg_newlocale_from_collation.md)
- Called from (representative examples):
  - [ProcessUtilitySlow](../P/ProcessUtilitySlow.md)

## Notes and Other Information
- Supports three collation providers: COLLPROVIDER_LIBC (default), COLLPROVIDER_ICU, and COLLPROVIDER_BUILTIN
- FROM syntax copies settings from an existing collation but prevents copying the "default" collation
- ICU collations support nondeterministic behavior and custom rules
- Performs extensive validation of parameter combinations and provider-specific requirements
- Automatically determines collation version if not explicitly provided
- Tests locale loading after creation to ensure the collation is functional