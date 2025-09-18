# lookup_rowtype_tupdesc_domain

## Location
src/backend/utils/cache/typcache.c: 1889 - 1925

## Overview
Looks up a TupleDesc for a row type, with special handling for domains over composite types, providing a faster alternative to calling getBaseType() followed by lookup_rowtype_tupdesc_noerror().

## Definition


## Detailed Description
This function extends the functionality of lookup_rowtype_tupdesc_noerror() by handling domains over named composite types transparently. When the input type is a domain, it automatically resolves to the base composite type and retrieves its TupleDesc. This optimization avoids the need for callers to explicitly call getBaseType() before looking up the tuple descriptor.

The function serves a critical role in PostgreSQL's type system by bridging domain types and their underlying composite structures. However, it intentionally keeps callers aware they might be dealing with a domain type, ensuring proper domain constraint handling when constructing tuples.

For RECORD types, it delegates to lookup_rowtype_tupdesc_internal(), while for other types it uses the type cache system to efficiently resolve both regular composite types and domain-wrapped composite types.

## Parameters / Member Variables
- : OID of the type to look up (can be a composite type, domain over composite type, or RECORDOID)
- : Type modifier value that may affect the specific variant of the type
- : If true, returns NULL on failure instead of throwing an error

## Dependencies
- Functions called/Symbols referenced:
  - [lookup_type_cache](lookup_type_cache.md)
  - [lookup_rowtype_tupdesc_noerror](lookup_rowtype_tupdesc_noerror.md)  
  - [lookup_rowtype_tupdesc_internal](lookup_rowtype_tupdesc_internal.md)
  - PinTupleDesc
- Called from (representative examples):
  - [ExecEvalWholeRowVar](../E/ExecEvalWholeRowVar.md) (src/backend/executor/execExprInterp.c:4843)
  - [rowtype_field_matches](../r/rowtype_field_matches.md) (src/backend/optimizer/util/clauses.c:2196)
  - [plperl_sv_to_datum](../p/plperl_sv_to_datum.md) (src/pl/plperl/plperl.c:1378)

## Notes and Other Information
- The function automatically pins the returned TupleDesc using PinTupleDesc() to prevent premature deallocation
- Unlike plain lookup_rowtype_tupdesc(), this variant intentionally exposes domain handling to callers
- Efficient caching is achieved through the type cache system with TYPECACHE_TUPDESC and TYPECACHE_DOMAIN_BASE_INFO flags
- Returns NULL when noError=true and the type is not composite, otherwise throws ERRCODE_WRONG_OBJECT_TYPE error