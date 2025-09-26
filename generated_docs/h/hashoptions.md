# hashoptions

## Location
src/backend/access/hash/hashutil.c: 275 - 290

## Overview
Parses and builds hash index relation options from a Datum, specifically handling the fillfactor parameter for hash indexes.

## Definition
```c
bytea *hashoptions(Datum reloptions, bool validate)
```

## Detailed Description
This function serves as the relation options parser for hash indexes, processing user-specified options and converting them into a structured format. Currently, it supports only the `fillfactor` option, which controls how full each page should be during initial index creation and subsequent operations.

The function uses PostgreSQL's standard relation options framework, defining a parsing table that maps the "fillfactor" string to an integer type stored in the HashOptions structure. It delegates the actual parsing and validation to the generic `build_reloptions` function while providing hash-specific parameters.

## Parameters
- `reloptions`: Datum containing the relation options text to parse
- `validate`: Boolean flag indicating whether to perform validation of option values

## Dependencies
- Functions called/Symbols referenced:
  - build_reloptions
  - relopt_parse_elt (structure type)
  - RELOPT_TYPE_INT (constant)
  - RELOPT_KIND_HASH (constant)
  - HashOptions (structure type)
  - lengthof (macro)
- Called from (representative examples):
  - hashhandler
  - HASHNProcs (function pointer array)

## Notes and Other Information
This function is part of PostgreSQL's extensible relation options system, allowing hash indexes to accept custom parameters during creation (e.g., CREATE INDEX ... WITH (fillfactor=70)). The fillfactor option controls storage density and can impact both performance and storage efficiency. While currently supporting only fillfactor, the structure allows for easy addition of future hash-specific options.