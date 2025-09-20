# brinoptions

## Location
[src/backend/access/brin/brin.c:1338-1355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin.c#L1338-L1355)

## Overview
Processes and validates relation options (reloptions) specific to BRIN indexes, handling the pages_per_range and autosummarize parameters.

## Definition

```c
bytea *
brinoptions(Datum reloptions, bool validate)
```
## Detailed Description
 is the reloptions processor function for BRIN indexes, responsible for parsing and validating index-specific options that can be specified during index creation or alteration. The function handles two key BRIN-specific options:

1. **pages_per_range**: An integer option that specifies how many heap pages each BRIN range should summarize (affects index granularity and size)
2. **autosummarize**: A boolean option that controls whether BRIN ranges should be automatically summarized when new data is inserted

The function uses PostgreSQL's standard reloptions infrastructure:
1. **Option Definition**: Defines a static table of supported options with their types and struct offsets
2. **Option Processing**: Calls  with the BRIN-specific option kind and BrinOptions structure size  
3. **Validation**: Performs validation if the validate parameter is true
4. **Return Format**: Returns the processed options as a bytea structure

## Parameters / Member Variables
- : Input Datum containing the raw reloptions text/array to be processed
- : Boolean flag indicating whether to perform validation of option values

## Dependencies
- Functions called/Symbols referenced:
  - : Core function for building reloptions structures
  - : Structure type for defining parseable relation options
  - : Structure containing BRIN-specific options (pagesPerRange, autosummarize)
  - : Option type constant for integer options
  - : Option type constant for boolean options  
  - : Option kind constant for BRIN indexes
  - : Macro for calculating array length
- Called from (representative examples):
  - : BRIN access method handler function

## Notes and Other Information
- Part of PostgreSQL's standard reloptions infrastructure for index access methods
- The  option directly affects BRIN index effectiveness and storage requirements
- The  option enables automatic maintenance of BRIN indexes during inserts
- Uses  to map option names to BrinOptions structure fields
- Returns NULL-equivalent if no options are specified or if processing fails
- Validation includes checking option value ranges and compatibility
- The returned bytea structure is used internally by BRIN functions to access option values