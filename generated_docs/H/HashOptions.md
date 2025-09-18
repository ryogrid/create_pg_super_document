# HashOptions

## Location
src/include/access/hash.h: 269 - 273

## Overview
HashOptions is a configuration structure that stores index-level options for PostgreSQL hash indexes, specifically controlling the fill factor parameter for hash index pages.

## Definition


## Detailed Description
HashOptions is a structure used to store reloptions (relation options) specific to hash indexes. It follows PostgreSQL's standard pattern for storing index options by including a varlena header and configuration parameters. The structure is used to customize the behavior of hash indexes at creation time through the WITH clause in CREATE INDEX statements.

The primary purpose is to control the fill factor, which determines how full each hash index page should be before a new page is allocated. A lower fill factor leaves more space for future insertions, potentially reducing page splits but using more disk space. A higher fill factor uses space more efficiently but may result in more page splits during insertions.

## Parameters / Member Variables
- : PostgreSQL's standard variable-length data header used for memory management and serialization (should not be manipulated directly)
- : The target fill factor for hash index pages, specified as a percentage (valid range: 0-100, default: 75)

## Dependencies
- Functions called/Symbols referenced:
  - No direct function calls (structure definition only)
- Called from (representative examples):
  -  (function in src/backend/access/hash/hashutil.c)
  -  (macro in src/include/access/hash.h)

## Notes and Other Information
- The fill factor must be between HASH_MIN_FILLFACTOR (10) and 100 percent
- Default fill factor is HASH_DEFAULT_FILLFACTOR (75)
- The HashGetFillFactor macro safely extracts the fill factor from a relation's rd_options field
- This structure is part of PostgreSQL's reloptions system, allowing users to customize index behavior via SQL DDL
- The varlena_header_ field enables the structure to be stored as a variable-length PostgreSQL datum
- Used exclusively with hash indexes (HASH_AM_OID access method)