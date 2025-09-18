# regnamespaceout

## Location
src/backend/utils/adt/regproc.c: 1718 - 1749

## Overview
The regnamespaceout function converts regnamespace OID values to their corresponding namespace (schema) names in text format for display and output purposes.

## Definition


## Detailed Description
This function is a PostgreSQL output function that handles the conversion of regnamespace OID values to human-readable namespace names. It serves as the counterpart to regnamespacein, providing the text representation of namespace references stored as OIDs. The function implements multiple output strategies to handle different cases gracefully.

When given an invalid OID (InvalidOid), the function returns the special value "-" to indicate an unknown or null namespace reference. For valid OIDs, it attempts to look up the corresponding namespace name using get_namespace_name. If a valid namespace is found, the name is properly quoted using quote_identifier to handle any special characters or reserved words safely.

If the OID doesn't correspond to any existing namespace in the system catalog, the function falls back to returning the numeric OID value as a string. This provides a stable representation even for stale or orphaned OID references, which can occur in certain database maintenance scenarios.

## Parameters / Member Variables
- The function uses PostgreSQL's standard function call interface (PG_FUNCTION_ARGS) which provides:
  - : Input OID value representing the namespace
  - : Output string containing the namespace name, "-", or numeric OID

## Dependencies
- Functions called/Symbols referenced:
  - PG_RETURN_CSTRING (returns the result string)
  - [get_namespace_name](../g/get_namespace_name.md) (looks up namespace name by OID)
  - [quote_identifier](../q/quote_identifier.md) (properly quotes namespace names)
  - NAMEDATALEN (constant for maximum name length)
- Called from (representative examples):
  - This function is typically invoked by PostgreSQL's type system when text output is needed for regnamespace values

## Notes and Other Information
- Returns "-" for invalid/null OID values (InvalidOid)
- Uses quote_identifier to safely handle namespace names with special characters or reserved words
- Falls back to numeric OID representation when namespace name lookup fails
- Part of the regnamespace type's input/output function pair with regnamespacein
- Provides stable output even for orphaned OID references that no longer exist in pg_namespace
- The pstrdup call is used to avoid compiler warnings about const string handling
- Located in src/backend/utils/adt/regproc.c with other reg* type functions
- Maximum output length is bounded by NAMEDATALEN for numeric fallback cases