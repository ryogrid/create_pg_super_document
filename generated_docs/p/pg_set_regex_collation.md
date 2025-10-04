# pg_set_regex_collation

## Location
[src/backend/regex/regc_pg_locale.c:234-293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_pg_locale.c#L234-L293)

## Overview
Sets the collation for regular expression operations to use throughout regex compilation and execution, configuring the appropriate locale strategy based on the collation provider and database encoding.

## Definition
```c
void pg_set_regex_collation(Oid collation)
```

## Detailed Description
This function is called at the beginning of regular expression compilation or execution to establish the collation context that all subsequent regex operations should follow. The function analyzes the provided collation OID and determines the most appropriate regex strategy to use, storing the results in static variables for the duration of the regex operation.

The function handles multiple collation scenarios:
- **C/POSIX collations**: Uses locale-C strategy regardless of database encoding
- **ICU collations**: Uses ICU-specific regex handling when available
- **UTF-8 databases**: Supports wide character operations with locale-specific or builtin providers
- **Single-byte encodings**: Uses single-byte locale strategies

The function also validates that the collation is deterministic, as nondeterministic collations are not supported for regular expressions.

## Parameters / Member Variables
- `collation`: The OID of the collation to use for regex operations. Must be a valid collation OID, otherwise an error is reported with a hint to use explicit COLLATE clause.

## Dependencies
- Functions called/Symbols referenced:
  - [lc_ctype_is_c](../l/lc_ctype_is_c.md)
  - [pg_newlocale_from_collation](pg_newlocale_from_collation.md)
  - [pg_locale_deterministic](pg_locale_deterministic.md)
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - ereport (for error handling)
- Called from (representative examples):
  - CNOERR (src/backend/regex/regcomp.c:403)
  - LOCALDfas (src/backend/regex/regexec.c:216)
  - [pg_regprefix](pg_regprefix.md) (src/backend/regex/regprefix.c:65)
  - GUTSMAGIC (src/include/regex/regguts.h:549)

## Notes and Other Information
- Uses static variables (pg_regex_strategy, pg_regex_locale, pg_regex_collation) to store collation state
- Non-reentrant by design since regex operations don't require reentrancy
- Supports multiple regex strategies: PG_REGEX_LOCALE_C, PG_REGEX_LOCALE_ICU, PG_REGEX_BUILTIN, PG_REGEX_LOCALE_WIDE_L, PG_REGEX_LOCALE_WIDE, PG_REGEX_LOCALE_1BYTE_L, PG_REGEX_LOCALE_1BYTE
- Validates collation determinism and reports specific errors for unsupported nondeterministic collations
- Location: src/backend/regex/regc_pg_locale.c:234-293

## Simplified Source

```c
void pg_set_regex_collation(Oid collation) {
    // Validate collation is specified
    if (!OidIsValid(collation)) {
        ereport(ERROR, "could not determine which collation to use for regular expression");
    }

    // Handle C/POSIX collations - use C strategy
    if (lc_ctype_is_c(collation)) {
        pg_regex_strategy = PG_REGEX_LOCALE_C;
        pg_regex_locale = 0;
        pg_regex_collation = C_COLLATION_OID;
        return;
    }

    // Get locale from collation and validate it's deterministic
    pg_regex_locale = pg_newlocale_from_collation(collation);
    if (!pg_locale_deterministic(pg_regex_locale)) {
        ereport(ERROR, "nondeterministic collations are not supported for regular expressions");
    }

    // Choose strategy based on provider and encoding
    if (pg_regex_locale && pg_regex_locale->provider == COLLPROVIDER_ICU) {
        pg_regex_strategy = PG_REGEX_LOCALE_ICU;
    } else if (GetDatabaseEncoding() == PG_UTF8) {
        // UTF-8 database: use wide character strategies
        if (pg_regex_locale) {
            pg_regex_strategy = (pg_regex_locale->provider == COLLPROVIDER_BUILTIN)
                               ? PG_REGEX_BUILTIN : PG_REGEX_LOCALE_WIDE_L;
        } else {
            pg_regex_strategy = PG_REGEX_LOCALE_WIDE;
        }
    } else {
        // Single-byte encoding: use 1-byte strategies
        pg_regex_strategy = pg_regex_locale ? PG_REGEX_LOCALE_1BYTE_L : PG_REGEX_LOCALE_1BYTE;
    }

    pg_regex_collation = collation;
}
```