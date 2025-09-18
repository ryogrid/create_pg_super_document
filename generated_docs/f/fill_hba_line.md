# fill_hba_line

## Location
[src/backend/utils/adt/hbafuncs.c:183-373](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/hbafuncs.c#L183-L373)

## Overview
Builds one row of the pg_hba_file_rules system view and adds it to a tuplestore, representing a single HBA configuration rule with all its parsed components.

## Definition


## Detailed Description
The  function constructs a complete row for the pg_hba_file_rules system view, which exposes PostgreSQL's host-based authentication configuration to SQL queries. It processes a parsed HBA line structure and extracts all relevant information including connection type, database names, user roles, network addresses, authentication methods, and options. The function handles both successful parsing results and error conditions, formatting network addresses appropriately and converting internal data structures into SQL-compatible formats. Memory leaks are acceptable since this runs in a short-lived memory context.

## Parameters / Member Variables
- : Tuplestore where the constructed row will be added
- : Tuple descriptor defining the structure of the pg_hba_file_rules view
- : Unique identifier for valid HBA rules (NULL for invalid rules)
- : Name of the HBA configuration file (always valid)
- : Line number within the configuration file (always valid)
- : Parsed HBA line data structure (can be NULL for parsing errors)
- : Error message for invalid rules (NULL if rule is valid)

## Dependencies
- Functions called/Symbols referenced:
  - [Int32GetDatum](../I/Int32GetDatum.md), CStringGetTextDatum, PointerGetDatum
  - [strlist_to_textarray](../s/strlist_to_textarray.md)
  - pg_getnameinfo_all
  - [clean_ipv6_addr](../c/clean_ipv6_addr.md)
  - [hba_authname](../h/hba_authname.md)
  - [get_hba_options](../g/get_hba_options.md)
  - [heap_form_tuple](../h/heap_form_tuple.md)
  - tuplestore_puttuple
- Types referenced:
  - Tuplestorestate, TupleDesc, HbaLine, AuthToken
  - NUM_PG_HBA_FILE_RULES_ATTS
  - Connection type enums (ctLocal, ctHost, ctHostSSL, etc.)
  - IP comparison method enums (ipCmpMask, ipCmpAll, etc.)
- Called from:
  - [fill_hba_view](fill_hba_view.md)

## Notes and Other Information
- Function is static and only used within hbafuncs.c for system view implementation
- Handles both valid and invalid HBA configuration lines appropriately
- Converts internal enum values to human-readable strings for SQL exposure
- Flattens AuthToken lists into string arrays without re-quoting for easier catalog comparison
- Uses PostgreSQL's standard tuple construction and storage mechanisms
- Network address formatting includes IPv6 address cleaning for proper display
- Memory management is simplified due to short-lived execution context