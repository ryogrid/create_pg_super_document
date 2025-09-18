# lexeme_hash

## Location
src/backend/tsearch/ts_typanalyze.c: 478 - 489

## Overview
A hash function for lexeme strings that handles non-null-terminated strings by using both the string data and its explicit length.

## Definition


## Detailed Description
This function computes hash values for lexemes stored in LexemeHashKey structures. Unlike standard string hashing functions that rely on null terminators, this function uses an explicit length field to handle lexeme strings that may not be null-terminated. It uses PostgreSQL's hash_any() function to compute the actual hash value from the lexeme bytes.

## Parameters / Member Variables
- : Pointer to LexemeHashKey structure containing lexeme data
- : Size of the key structure (unused, length comes from key->length)

## Dependencies
- Functions called/Symbols referenced:
  - LexemeHashKey
  - hash_any
  - DatumGetUInt32
- Called from (representative examples):
  - compute_tsvector_stats (via hash table configuration)

## Notes and Other Information
- Designed for lexemes that are not NULL terminated strings
- Uses explicit length from LexemeHashKey->length field
- Part of hash table setup for Lossy Counting algorithm in tsvector statistics
- Returns uint32 hash value suitable for PostgreSQL hash tables
- The keysize parameter is not used since length is embedded in the key structure