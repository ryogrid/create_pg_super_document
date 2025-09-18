# TypeCacheEntry

## Location
src/include/utils/typcache.h: 31 - 134

## Overview
TypeCacheEntry is a comprehensive data structure that caches type-related information for PostgreSQL data types to avoid repeated lookups and computations during query execution.

## Definition


## Detailed Description
TypeCacheEntry serves as a comprehensive caching mechanism for type-related metadata in PostgreSQL's type system. This structure eliminates the need for repeated catalog lookups and expensive function setup operations by maintaining pre-computed information about data types, their operators, and associated functions.

The cache entry is organized into several logical sections: basic type properties copied from pg_type, operator family information for comparison and hashing operations, pre-initialized function manager information for frequently used operations, and specialized data for complex types like ranges, domains, enums, and composite types.

The structure is designed with performance in mind - the type_id field must be first to serve as the hash key, and frequently accessed function information is pre-initialized to avoid repeated fmgr_info() calls during query execution.

## Parameters / Member Variables
### Core Type Information
- : The OID of the data type, serves as the primary hash lookup key
- : Pre-computed hash value of the type OID for faster lookups
- : Length of the type (-1 for variable length, -2 for cstring)
- : Whether the type is passed by value or reference
- : Alignment requirement for the type ('c', 's', 'i', 'd')
- : Storage strategy ('p'lain, 'e'xternal, 'm'ain, 'x'tended)
- : Type category ('b'ase, 'c'omposite, 'd'omain, 'e'num, 'p'seudo, 'r'ange, 'm'ultirange)
- : OID of the relation if this is a composite type
- : OID of the subscripting handler function
- : OID of the element type for arrays
- : OID of the default collation for the type

### Operator Family Information
- : Default B-tree operator class family OID
- : Input type OID for the B-tree operator class
- : Default hash operator class family OID
- : Input type OID for the hash operator class
- : OID of the equality operator
- : OID of the less-than operator
- : OID of the greater-than operator
- : OID of the B-tree comparison function
- : OID of the hash calculation function
- : OID of the extended hash calculation function

### Pre-initialized Function Information
- : Pre-setup function manager info for equality operator
- : Pre-setup function manager info for comparison function
- : Pre-setup function manager info for hash function
- : Pre-setup function manager info for extended hash function

### Composite Type Information
- : Tuple descriptor for composite types (reference-counted)
- : Unique identifier for the tuple descriptor lifetime

### Range Type Information
- : Pointer to the TypeCacheEntry of the range's element type
- : Operator family OID for range comparisons
- : Collation OID for range comparisons
- : Pre-setup comparison function info for ranges
- : Pre-setup canonicalization function info for ranges
- : Pre-setup difference function info for ranges

### Multirange Type Information
- : Pointer to the TypeCacheEntry of the underlying range type

### Domain Type Information
- : OID of the base type for domain types
- : Type modifier of the base type for domain types
- : Pointer to domain constraint cache data

### Internal Management
- : Bit flags indicating which information has been computed
- : Pointer to enum-specific cached data
- : Pointer to next domain type entry in linked list

## Dependencies
- Functions called/Symbols referenced:
  - [DomainConstraintCache](../D/DomainConstraintCache.md)
  - [TypeCacheEnumData](TypeCacheEnumData.md)
- Called from (representative examples):
  - Various functions in typcache.c
  - Type comparison and hashing operations throughout PostgreSQL

## Notes and Other Information
- The type_id field must be positioned first in the structure as it serves as the hash lookup key
- Function manager information is pre-initialized to avoid memory leaks in repeated function calls
- The tuple descriptor for composite types is reference-counted and must be managed carefully
- Domain types are maintained in a linked list via the nextDomain pointer
- The flags field uses bit patterns to track which optional information has been computed and cached
- This structure is central to PostgreSQL's type system performance, avoiding repeated catalog lookups during query execution