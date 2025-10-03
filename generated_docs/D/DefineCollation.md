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
- `*pstate`: ParseState for error reporting with location information
- `*names`: List of qualified names specifying the collation name and schema
- `*parameters`: List of DefElem structures containing collation options
- `if_not_exists`: Boolean flag for IF NOT EXISTS behavior
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

## Simplified Source

```c
ObjectAddress
DefineCollation(ParseState *pstate, List *names, List *parameters, bool if_not_exists)
{
    char *collName;
    Oid collNamespace;

    // Parse collation definition elements
    DefElem *fromEl = NULL;
    DefElem *localeEl = NULL;
    DefElem *lccollateEl = NULL;
    DefElem *lcctypeEl = NULL;
    DefElem *providerEl = NULL;
    DefElem *deterministicEl = NULL;
    DefElem *rulesEl = NULL;
    DefElem *versionEl = NULL;

    // Collation properties
    char *collcollate = NULL;
    char *collctype = NULL;
    const char *colllocale = NULL;
    char *collicurules = NULL;
    bool collisdeterministic = true;
    char collprovider = COLLPROVIDER_LIBC;
    char *collversion = NULL;
    int collencoding;

    // Extract name and validate namespace permissions
    collNamespace = QualifiedNameGetCreationNamespace(names, &collName);
    check_namespace_permissions(collNamespace);

    // Parse all parameters from CREATE COLLATION command
    foreach(pl, parameters) {
        DefElem *defel = lfirst_node(DefElem, pl);

        if (strcmp(defel->defname, "from") == 0)
            fromEl = defel;
        else if (strcmp(defel->defname, "locale") == 0)
            localeEl = defel;
        else if (strcmp(defel->defname, "lc_collate") == 0)
            lccollateEl = defel;
        else if (strcmp(defel->defname, "lc_ctype") == 0)
            lcctypeEl = defel;
        else if (strcmp(defel->defname, "provider") == 0)
            providerEl = defel;
        else if (strcmp(defel->defname, "deterministic") == 0)
            deterministicEl = defel;
        else if (strcmp(defel->defname, "rules") == 0)
            rulesEl = defel;
        else if (strcmp(defel->defname, "version") == 0)
            versionEl = defel;
        // Check for conflicts and duplicates
    }

    // Validate parameter combinations
    if (localeEl && (lccollateEl || lcctypeEl))
        ereport(ERROR, "LOCALE cannot be used with LC_COLLATE or LC_CTYPE");

    if (fromEl && list_length(parameters) != 1)
        ereport(ERROR, "FROM cannot be used with other options");

    if (fromEl) {
        // Copy from existing collation
        Oid collid = get_collation_oid(defGetQualifiedName(fromEl), false);

        // Get existing collation properties from system catalog
        HeapTuple tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));

        // Extract properties: provider, deterministic, encoding, locale info
        collprovider = ((Form_pg_collation) GETSTRUCT(tp))->collprovider;
        collisdeterministic = ((Form_pg_collation) GETSTRUCT(tp))->collisdeterministic;
        collencoding = ((Form_pg_collation) GETSTRUCT(tp))->collencoding;

        // Get locale strings from existing collation
        extract_collation_locale_info(tp, &collcollate, &collctype,
                                     &colllocale, &collicurules);

        ReleaseSysCache(tp);

        // Cannot copy the "default" collation
        if (collprovider == COLLPROVIDER_DEFAULT)
            ereport(ERROR, "collation \"default\" cannot be copied");
    } else {
        // Create with explicit parameters

        // Set provider
        if (providerEl) {
            char *providerstr = defGetString(providerEl);
            if (pg_strcasecmp(providerstr, "builtin") == 0)
                collprovider = COLLPROVIDER_BUILTIN;
            else if (pg_strcasecmp(providerstr, "icu") == 0)
                collprovider = COLLPROVIDER_ICU;
            else if (pg_strcasecmp(providerstr, "libc") == 0)
                collprovider = COLLPROVIDER_LIBC;
            else
                ereport(ERROR, "unrecognized collation provider: %s", providerstr);
        }

        // Set deterministic flag
        if (deterministicEl)
            collisdeterministic = defGetBoolean(deterministicEl);

        // Set locale information based on provider
        if (localeEl) {
            if (collprovider == COLLPROVIDER_LIBC) {
                collcollate = defGetString(localeEl);
                collctype = defGetString(localeEl);
            } else {
                colllocale = defGetString(localeEl);
            }
        }

        // Set individual LC_COLLATE and LC_CTYPE if specified
        if (lccollateEl)
            collcollate = defGetString(lccollateEl);
        if (lcctypeEl)
            collctype = defGetString(lcctypeEl);

        // Validate provider-specific requirements
        validate_collation_parameters(collprovider, colllocale, collcollate,
                                    collctype, collisdeterministic, rulesEl);

        // Set encoding based on provider
        determine_collation_encoding(collprovider, &collencoding);
    }

    // Get or generate collation version
    if (!collversion) {
        const char *locale = (collprovider == COLLPROVIDER_LIBC) ?
                           collcollate : colllocale;
        collversion = get_collation_actual_version(collprovider, locale);
    }

    // Create the collation in system catalog
    Oid newoid = CollationCreate(collName, collNamespace, GetUserId(),
                                collprovider, collisdeterministic, collencoding,
                                collcollate, collctype, colllocale, collicurules,
                                collversion, if_not_exists, false);

    if (!OidIsValid(newoid))
        return InvalidObjectAddress;

    // Test that the locale can be loaded
    CommandCounterIncrement();
    if (!lc_collate_is_c(newoid) || !lc_ctype_is_c(newoid))
        (void) pg_newlocale_from_collation(newoid);

    ObjectAddress address;
    ObjectAddressSet(address, CollationRelationId, newoid);
    return address;
}
```