# plperl_sv_to_literal

## Location
src/pl/plperl/plperl.c: 1444 - 1479

## Overview
Converts a Perl scalar value (SV) to its string literal representation by using the PostgreSQL type output function.

## Definition
char *plperl_sv_to_literal(SV *sv, char *fqtypename)

## Detailed Description
This function serves as a bridge between Perl and PostgreSQL type systems by converting a Perl scalar value to its string representation according to PostgreSQL type conventions. It performs the conversion in several steps: first resolving the type name to a PostgreSQL type OID, then converting the Perl SV to a PostgreSQL Datum using the appropriate type conversion, and finally applying the type output function to produce the string representation. This is essential for properly formatting Perl values when they need to be represented as SQL literals.

## Parameters / Member Variables
- `sv`: Perl scalar value to be converted to string literal
- `fqtypename`: Fully qualified PostgreSQL type name (e.g., "integer", "text", "timestamp")

## Dependencies
- Functions called/Symbols referenced:
  - check_spi_usage_allowed
  - DirectFunctionCall1
  - regtypein
  - CStringGetDatum
  - plperl_sv_to_datum
  - getTypeOutputInfo
  - OidOutputFunctionCall
- Called from (representative examples):
  - Function is declared in plperl.h for external use

## Notes and Other Information
- Returns NULL if the converted value is SQL NULL
- Throws an error if the type name cannot be resolved to a valid PostgreSQL type OID
- Requires SPI (Server Programming Interface) usage to be allowed in the current context
- The returned string is allocated in the current memory context and should be managed accordingly
- Essential for PL/Perl ability to return properly formatted values to PostgreSQL