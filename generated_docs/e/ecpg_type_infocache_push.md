# ecpg_type_infocache_push

## Location
[src/interfaces/ecpg/ecpglib/execute.c:148-163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/ecpglib/execute.c#L148-L163)

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
  - [ECPGtype_information_cache](../E/ECPGtype_information_cache.md) (struct type)
  - ARRAY_TYPE (enum type)
  - [ecpg_alloc](ecpg_alloc.md)
- Called from (representative examples):
  - not_an_array_in_ecpg (many locations)

## Notes and Other Information
- Returns true on successful allocation and insertion, false on allocation failure
- Implements stack-like insertion (new entries added at the front)
- Part of ECPG's type system for tracking PostgreSQL data type information
- Used extensively for caching type metadata to optimize type handling
- Memory allocation failure is gracefully handled by returning false

## Simplified Source

```c
static bool
ecpg_type_infocache_push(struct ECPGtype_information_cache **cache, int oid, enum ARRAY_TYPE isarray, int lineno)
{
    // Allocate new cache entry
    struct ECPGtype_information_cache *new_entry =
        ecpg_alloc(sizeof(struct ECPGtype_information_cache), lineno);

    if (new_entry == NULL)
        return false;

    // Initialize entry and add to front of list
    new_entry->oid = oid;
    new_entry->isarray = isarray;
    new_entry->next = *cache;
    *cache = new_entry;

    return true;
}
```