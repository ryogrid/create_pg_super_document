# SpGistOptions

## Location
src/include/access/spgist_private.h: 26 - 30

## Overview
SpGistOptions is a structure that holds configuration options for SP-GiST (Space-Partitioned Generalized Search Tree) indexes, specifically the fill factor parameter.

## Definition


## Detailed Description
SpGistOptions is a configuration structure used to store index-specific options for SP-GiST indexes. The structure follows PostgreSQL's standard pattern for index option structures by including a varlena header for variable-length data handling. The primary purpose is to encapsulate the fill factor setting, which controls how densely packed the index pages should be during index creation and maintenance operations.

## Parameters / Member Variables
- `varlena_header_`: A varlena header field required for PostgreSQL's variable-length data infrastructure. This field should not be accessed directly by user code.
- `fillfactor`: An integer representing the page fill factor as a percentage (valid range 0-100). This controls how much of each index page should be filled during index creation, leaving space for future insertions.

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references from this symbol)
- Called from (representative examples):
  - [spgoptions](../s/spgoptions.md) (in src/backend/access/spgist/spgutils.c)
  - SpGistGetFillFactor (in src/include/access/spgist_private.h)

## Notes and Other Information
- This structure is part of PostgreSQL's SP-GiST access method implementation
- The fill factor setting affects index performance by balancing space utilization against the frequency of page splits
- Lower fill factors leave more free space for insertions but use more disk space
- Higher fill factors use space more efficiently but may cause more frequent page splits during insertions