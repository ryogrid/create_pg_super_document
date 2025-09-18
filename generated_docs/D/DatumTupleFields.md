# DatumTupleFields

## Location
[src/include/access/htup_details.h:134-151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/access/htup_details.h#L134-L151)

## Overview
DatumTupleFields is a structure that represents the metadata fields for tuple datums in PostgreSQL, containing type identification and length information for composite type values.

## Definition


## Detailed Description
DatumTupleFields serves as a header structure for tuple datums that need to carry type information along with the actual data. This structure is used when PostgreSQL needs to store composite type values as datums, providing essential metadata about the type system information. The structure ensures that composite type values can be properly identified and processed throughout the system.

The structure is designed with careful field ordering considerations, anticipating potential future expansion of the Oid type to 64 bits. It maintains compatibility with the varlena header system while providing specific composite type identification.

## Parameters / Member Variables
- : varlena header field that should not be accessed directly, used for variable-length data management
- : type modifier value, either -1 or an identifier for a specific record type variant  
- : the Object Identifier (OID) of the composite type, or RECORDOID for anonymous record types

## Dependencies
- Functions called/Symbols referenced: None directly
- Called from (representative examples):
  - [HeapTupleHeaderData](../H/HeapTupleHeaderData.md) (used as a union member)

## Notes and Other Information
- The datum_typeid field cannot represent a domain over composite types, only plain composite types, following PostgreSQL's principle that CoerceToDomain does not alter the physical representation of base type values
- Field ordering is deliberately chosen with consideration for potential future expansion of the Oid type to 64 bits
- This structure is part of the tuple header system and works in conjunction with HeapTupleHeaderData
- Located in src/include/access/htup_details.h:134-151