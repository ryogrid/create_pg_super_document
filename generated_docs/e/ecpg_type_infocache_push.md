# ecpg_type_infocache_push

## Location
src/interfaces/ecpg/ecpglib/execute.c: 148 - 163

## Overview
A static utility function that adds a new type information entry to the front of an ECPG type information cache linked list.

## Definition
```c
static bool ecpg_type_infocache_push(struct ECPGtype_information_cache **cache, int oid, enum ARRAY_TYPE isarray, int lineno)
```

## Detailed Description
The `ecpg_type_infocache_push` function creates a new type information cache entry and adds it to the beginning of a linked list cache. This cache is used to store PostgreSQL type OID information along with whether each type represents an array type. The function allocates memory for the new entry using ECPG's memory management system and updates the cache pointer to point to the new entry, effectively implementing a stack-like LIFO (Last In, First Out) insertion pattern.

## Parameters / Member Variables
- `cache`: Double pointer to the head of the type information cache linked list
- `oid`: PostgreSQL object identifier (OID) for the data type
- `isarray`: Enumeration value indicating the array type classification
- `lineno`: Line number for memory allocation tracking (used by ecpg_alloc)

## Dependencies
- Functions called/Symbols referenced:
  - ECPGtype_information_cache (struct type)
  - ARRAY_TYPE (enum type)
  - ecpg_alloc
- Called from (representative examples):
  - not_an_array_in_ecpg (many locations)

## Notes and Other Information
- Returns true on successful allocation and insertion, false on allocation failure
- Implements stack-like insertion (new entries added at the front)
- Part of ECPG's type system for tracking PostgreSQL data type information
- Used extensively for caching type metadata to optimize type handling
- Memory allocation failure is gracefully handled by returning false