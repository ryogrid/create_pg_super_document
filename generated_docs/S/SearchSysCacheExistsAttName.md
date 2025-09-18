# SearchSysCacheExistsAttName

## Location
src/backend/utils/cache/syscache.c: 523 - 543

## Overview
Checks whether an attribute with the given name exists in a relation, ignoring dropped attributes.

## Definition


## Detailed Description
This function is an attisdropped-aware version of SearchSysCacheExists that specifically checks for the existence of an attribute by name within a given relation. It uses SearchSysCacheAttName internally to perform the lookup, which automatically excludes attributes that have been marked as dropped (attisdropped = true). This provides a convenient way for callers to test attribute existence while treating dropped attributes as if they don't exist.

The function returns true if a valid, non-dropped attribute with the specified name exists in the relation, and false otherwise.

## Parameters / Member Variables
- : The OID of the relation to search for the attribute
- : The name of the attribute to look for

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCacheAttName](SearchSysCacheAttName.md)
  - HeapTupleIsValid
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - [RemoveInheritance](../R/RemoveInheritance.md) (src/backend/commands/tablecmds.c:16319)

## Notes and Other Information
- This function is specifically designed to ignore dropped attributes, making it safer for use in contexts where dropped attributes should be treated as non-existent
- The function properly releases the system cache tuple if found, preventing memory leaks
- Part of the PostgreSQL system cache infrastructure for efficient catalog lookups
- Returns a simple boolean result, making it convenient for existence checks without needing to handle HeapTuple objects