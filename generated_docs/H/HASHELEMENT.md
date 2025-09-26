# HASHELEMENT

## Location
src/include/utils/hsearch.h: 51 - 55

## Overview
HASHELEMENT is the private header structure that prefixes every entry in PostgreSQL's hash tables, providing linkage and hash value storage for efficient bucket-based organization.

## Definition


## Detailed Description
HASHELEMENT serves as the internal metadata header for entries in PostgreSQL's dynamic hash table implementation. It is prepended to every hash table entry and is invisible to the caller. The actual user data follows this structure on a MAXALIGN'd boundary, with the hash key expected to be at the start of the caller's data structure.

This design enables efficient hash table operations by maintaining bucket chains through the link pointer and storing the computed hash value for quick comparisons during lookups. The structure facilitates collision resolution through chaining, where entries with the same hash bucket are linked together.

## Parameters / Member Variables
- : Pointer to the next HASHELEMENT in the same hash bucket, forming a singly-linked list for collision resolution
- : The 32-bit hash function result computed for this entry, stored to avoid recomputation during operations

## Dependencies
- Functions called/Symbols referenced:
  - struct HASHELEMENT (self-reference for linked list)
- Called from (representative examples):
  - HASHBUCKET
  - HASHSEGMENT
  - ELEMENTKEY
  - ELEMENT_FROM_KEY
  - hash_seq_search
  - element_alloc

## Notes and Other Information
- The HASHELEMENT structure is always followed by the caller's data on a MAXALIGN'd boundary
- The hash key must be positioned at the beginning of the caller's data structure
- This is an internal structure not directly manipulated by hash table users
- The design enables efficient memory layout with metadata and user data stored contiguously
- Used extensively in PostgreSQL's dynahash.c implementation for all hash table operations