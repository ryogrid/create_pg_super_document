# GetTSConfigTuple

## Location
src/backend/commands/tsearchcmds.c: 787 - 811

## Overview
Finds and retrieves the system catalog tuple for a text search configuration by name, returning NULL if no such configuration exists.

## Definition


## Detailed Description
GetTSConfigTuple is a static utility function that performs a two-step lookup to find a text search configuration in the system catalog. It first resolves the configuration name to an OID using get_ts_config_oid(), then retrieves the corresponding tuple from the system cache using SearchSysCache1(). The function is designed to handle missing configurations gracefully by returning NULL, making it suitable for operations where the configuration may not exist.

## Parameters / Member Variables
- : A List containing the qualified or unqualified name components of the text search configuration to look up

## Dependencies
- Functions called/Symbols referenced:
  - get_ts_config_oid (converts name list to OID)
  - OidIsValid (validates OID)
  - SearchSysCache1 (retrieves tuple from system cache)
  - HeapTupleIsValid (validates tuple)
  - ObjectIdGetDatum (converts OID to Datum)
  - elog (error logging)
- Called from (representative examples):
  - AlterTSConfiguration

## Notes and Other Information
- Static function, only accessible within tsearchcmds.c
- Uses the TSCONFIGOID cache for efficient lookups
- Includes error handling for cache lookup failures with detailed error message
- The function comment indicates that cache lookup failure "should not happen"
- Returns HeapTuple that must be released by caller using ReleaseSysCache