# HashPageOpaqueData

## Location
src/include/access/hash.h: 77 - 84

## Overview
HashPageOpaqueData is a structure that stores metadata in the opaque area of hash index pages, containing information about bucket chains and page identification.

## Definition


## Detailed Description
HashPageOpaqueData defines the structure of the opaque area in hash index pages. This structure maintains critical metadata for bucket chain navigation and page identification. The structure supports the linked-list organization of hash bucket pages, where overflow pages can be chained together when a bucket needs to store more items than fit on a single page.

The design allows for efficient traversal of bucket chains while also providing mechanisms to detect stale cached metapage information without requiring locks on the metapage itself.

## Parameters / Member Variables
- : In overflow pages, stores the block number of the previous page in the bucket chain. In bucket pages, stores the hashm_maxbucket value from the last bucket split or creation time (used for metapage staleness detection)
- : Block number of the next page in the bucket chain, or InvalidBlockNumber if this is the last page
- : The bucket number that this page belongs to
- : Page type code combined with flag bits for page classification
- : Identifier used for hash index validation and identification

## Dependencies
- Functions called/Symbols referenced:
  - Bucket
  - BlockNumber
- Called from (representative examples):
  - [_hash_pageinit](../h/_hash_pageinit.md)
  - _hash_checkpage
  - HashPageOpaque
  - HashMaxItemSize
  - HashGetMaxBitmapSize

## Notes and Other Information
The dual use of hasho_prevblkno (for both chain linking and metapage staleness detection) is a clever optimization that avoids the need to lock the metapage when determining if cached information is still valid. This design is particularly important for performance in high-concurrency scenarios where multiple processes are accessing the hash index simultaneously.