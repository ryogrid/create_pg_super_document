# get_language_name

## Location
src/bin/pg_dump/pg_dump.c: 8691 - 8713

## Overview
Retrieves the name of a procedural language from the system cache given its OID, with optional error handling for missing languages.

## Definition


## Detailed Description
The  function performs a cached lookup in the  system catalog to retrieve the name of a procedural language identified by its OID. It uses PostgreSQL's system cache mechanism for efficient access to frequently-needed language information. The function can optionally handle missing languages gracefully based on the  parameter.

When a language is found, the function extracts the language name from the  tuple and returns a palloc'd copy. If the language OID doesn't exist and  is false, it throws an ERROR. If  is true, it returns NULL for non-existent languages.

## Parameters / Member Variables
- : The OID of the language to look up in the pg_language catalog
- : If true, return NULL for non-existent languages instead of throwing an error

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - HeapTupleIsValid
  - Form_pg_language
  - GETSTRUCT
  - pstrdup
  - NameStr
  - ReleaseSysCache
  - elog
- Called from (representative examples):
  - getObjectDescription
  - getObjectIdentityParts
  - get_transform_oid
  - pg_get_functiondef
  - getTransforms
  - dumpTransform

## Notes and Other Information
- Part of the language cache subsystem in lsyscache.c
- Uses the LANGOID cache for efficient lookup of language information
- Returns a palloc'd string that must be freed by the caller
- Commonly used in object description functions and pg_dump operations
- The returned string contains only the language name, not the full language definition