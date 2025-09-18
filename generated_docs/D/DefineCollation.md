# DefineCollation

## Location
src/backend/commands/collationcmds.c: 53 - 399

## Overview
DefineCollation implements the CREATE COLLATION SQL command, creating a new collation object in the PostgreSQL system catalog with specified locale and provider settings.

## Definition


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
  - QualifiedNameGetCreationNamespace
  - object_aclcheck
  - get_collation_oid
  - builtin_validate_locale
  - icu_language_tag
  - icu_validate_locale
  - get_collation_actual_version
  - CollationCreate
  - pg_newlocale_from_collation
- Called from (representative examples):
  - ProcessUtilitySlow

## Notes and Other Information
- Supports three collation providers: COLLPROVIDER_LIBC (default), COLLPROVIDER_ICU, and COLLPROVIDER_BUILTIN
- FROM syntax copies settings from an existing collation but prevents copying the "default" collation
- ICU collations support nondeterministic behavior and custom rules
- Performs extensive validation of parameter combinations and provider-specific requirements
- Automatically determines collation version if not explicitly provided
- Tests locale loading after creation to ensure the collation is functional