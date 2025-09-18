# BlocktableEntry

## Location
src/backend/access/common/tidstore.c: 44 - 77

## Overview
BlocktableEntry is a data structure that represents entries in the block table of PostgreSQL's TidStore, designed to efficiently store tuple identifiers (TIDs) for a specific block, with optimizations for both sparse and dense offset distributions.

## Definition


## Detailed Description
BlocktableEntry serves as the core data structure for storing tuple identifiers within a single block in PostgreSQL's TidStore implementation. It is designed with a dual storage strategy: for sparse distributions of offsets, it uses a compact array () to store individual offset numbers directly, while for denser distributions, it employs a bitmap representation using the  array. The structure is optimized for memory efficiency and includes careful consideration for endianness and alignment requirements. The design is similar to PagetableEntry in tidbitmap.c, sharing architectural patterns for efficient TID storage.

## Parameters / Member Variables
- : Control flags that indicate the storage mode and other metadata about the entry
- : Number of bitmap words used in the  array when using bitmap storage mode
- : Array for storing individual offset numbers directly in sparse cases
- : Variable-length array of bitmap words for dense offset storage

## Dependencies
- Functions called/Symbols referenced:
  - int8 (data type)
  - NUM_FULL_OFFSETS (constant)
  - FLEXIBLE_ARRAY_MEMBER (macro)
  - bitmapword (data type)
- Called from (representative examples):
  - [TidStoreSetBlockOffsets](../T/TidStoreSetBlockOffsets.md)
  - [TidStoreIsMember](../T/TidStoreIsMember.md)
  - [TidStoreIterateNext](../T/TidStoreIterateNext.md)
  - [tidstore_iter_extract_tids](../t/tidstore_iter_extract_tids.md)

## Notes and Other Information
- The structure layout is carefully designed to handle endianness differences, with conditional compilation directives ensuring proper memory alignment
- The backing radix tree can tag the lowest bit when the header is stored inside a pointer or DSA pointer, requiring specific positioning of the flags member
- Memory layout is optimized to avoid padding space between header and words array
- Code creating new entries should zero out space up to the 'words' member to ensure proper initialization
- The flexible array member allows for variable-length bitmap storage depending on the density of offsets in the block