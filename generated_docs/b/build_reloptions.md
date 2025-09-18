# build_reloptions

## Location
src/backend/access/common/reloptions.c: 1917 - 1953

## Overview
A core function that parses relation options from a Datum and builds a structured options object, coordinating the complete parsing and allocation process.

## Definition


## Detailed Description
This function serves as the main entry point for building relation options structures. It orchestrates the complete process of parsing raw relation options: first calling parseRelOptions to convert the input Datum into structured relopt_value entries, then allocating memory for the result structure using allocateReloptStruct, and finally filling the structure with parsed values using fillRelOptions. The function handles the case where no options are provided by returning NULL, and properly manages memory by freeing the intermediate parsed options array.

## Parameters / Member Variables
- : Input Datum containing the raw relation options to be parsed
- : Must be true if reloptions is freshly built by transformRelOptions(), false if read from catalog with pre-validated values
- : The specific kind of relation options being processed (relopt_kind enum)
- : Size of the target options structure to be allocated
- : Parsing table describing allowed options and their properties
- : Number of elements in the parsing table

## Dependencies
- Functions called/Symbols referenced:
  - relopt_kind (enum type)
  - relopt_parse_elt (struct type)
  - relopt_value (struct type)
  - parseRelOptions (function)
  - allocateReloptStruct (function)
  - fillRelOptions (function)
  - Assert (macro)
  - pfree (PostgreSQL memory management function)
- Called from:
  - brinoptions (src/backend/access/brin/brin.c:1345)
  - default_reloptions (src/backend/access/common/reloptions.c:1895)
  - view_reloptions (src/backend/access/common/reloptions.c:2018)
  - attribute_reloptions (src/backend/access/common/reloptions.c:2085)
  - tablespace_reloptions (src/backend/access/common/reloptions.c:2104)
  - ginoptions (src/backend/access/gin/ginutil.c:610)
  - gistoptions (src/backend/access/gist/gistutil.c:918)
  - hashoptions (src/backend/access/hash/hashutil.c:281)
  - btoptions (src/backend/access/nbtree/nbtutils.c:4573)
  - spgoptions (src/backend/access/spgist/spgutils.c:757)
  - GET_STRING_RELOPTION (src/include/access/reloptions.h:228)
  - dioptions (src/test/modules/dummy_index_am/dummy_index_am.c:226)

## Notes and Other Information
- Returns NULL if no options were provided or matched, unless validate=true which would cause an error
- Uses Assert to verify that the number of parsed options doesn't exceed the parsing table size
- Properly manages memory by freeing the intermediate options array after structure filling
- The validate parameter is crucial for distinguishing between fresh user input (which needs validation) and catalog-stored values (which are pre-validated)
- This function is used by virtually all PostgreSQL access methods and relation types for option parsing
- The returned structure is dynamically allocated and becomes the caller's responsibility to manage