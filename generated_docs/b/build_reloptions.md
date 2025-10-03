# build_reloptions

## Location
[src/backend/access/common/reloptions.c:1917-1953](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/reloptions.c#L1917-L1953)

## Overview
A core function that parses relation options from a Datum and builds a structured options object, coordinating the complete parsing and allocation process.

## Definition

```c
void *
build_reloptions(Datum reloptions, bool validate,
				 relopt_kind kind,
				 Size relopt_struct_size,
				 const relopt_parse_elt *relopt_elems,
				 int num_relopt_elems)
```
## Detailed Description
This function serves as the main entry point for building relation options structures. It orchestrates the complete process of parsing raw relation options: first calling parseRelOptions to convert the input Datum into structured relopt_value entries, then allocating memory for the result structure using allocateReloptStruct, and finally filling the structure with parsed values using fillRelOptions. The function handles the case where no options are provided by returning NULL, and properly manages memory by freeing the intermediate parsed options array.

## Parameters / Member Variables
- `reloptions`: Input Datum containing the raw relation options to be parsed
- `validate`: Must be true if reloptions is freshly built by transformRelOptions(), false if read from catalog with pre-validated values
- `kind`: The specific kind of relation options being processed (relopt_kind enum)
- `relopt_struct_size`: Size of the target options structure to be allocated
- `*relopt_elems`: Parsing table describing allowed options and their properties
- `num_relopt_elems`: Number of elements in the parsing table
## Dependencies
- Functions called/Symbols referenced:
  - relopt_kind (enum type)
  - relopt_parse_elt (struct type)
  - [relopt_value](../r/relopt_value.md) (struct type)
  - [parseRelOptions](../p/parseRelOptions.md) (function)
  - [allocateReloptStruct](../a/allocateReloptStruct.md) (function)
  - [fillRelOptions](../f/fillRelOptions.md) (function)
  - Assert (macro)
  - [pfree](../p/pfree.md) (PostgreSQL memory management function)
- Called from:
  - [brinoptions](brinoptions.md) (src/backend/access/brin/brin.c:1345)
  - [default_reloptions](../d/default_reloptions.md) (src/backend/access/common/reloptions.c:1895)
  - [view_reloptions](../v/view_reloptions.md) (src/backend/access/common/reloptions.c:2018)
  - [attribute_reloptions](../a/attribute_reloptions.md) (src/backend/access/common/reloptions.c:2085)
  - [tablespace_reloptions](../t/tablespace_reloptions.md) (src/backend/access/common/reloptions.c:2104)
  - [ginoptions](../g/ginoptions.md) (src/backend/access/gin/ginutil.c:610)
  - [gistoptions](../g/gistoptions.md) (src/backend/access/gist/gistutil.c:918)
  - [hashoptions](../h/hashoptions.md) (src/backend/access/hash/hashutil.c:281)
  - [btoptions](btoptions.md) (src/backend/access/nbtree/nbtutils.c:4573)
  - [spgoptions](../s/spgoptions.md) (src/backend/access/spgist/spgutils.c:757)
  - GET_STRING_RELOPTION (src/include/access/reloptions.h:228)
  - [dioptions](../d/dioptions.md) (src/test/modules/dummy_index_am/dummy_index_am.c:226)

## Notes and Other Information
- Returns NULL if no options were provided or matched, unless validate=true which would cause an error
- Uses Assert to verify that the number of parsed options doesn't exceed the parsing table size
- Properly manages memory by freeing the intermediate options array after structure filling
- The validate parameter is crucial for distinguishing between fresh user input (which needs validation) and catalog-stored values (which are pre-validated)
- This function is used by virtually all PostgreSQL access methods and relation types for option parsing
- The returned structure is dynamically allocated and becomes the caller's responsibility to manage

## Simplified Source

```c
void *build_reloptions(Datum reloptions, bool validate,
                       relopt_kind kind, Size relopt_struct_size,
                       const relopt_parse_elt *relopt_elems,
                       int num_relopt_elems) {
    int numoptions;
    relopt_value *options;
    void *rdopts;

    // Parse options specific to given relation option kind
    options = parseRelOptions(reloptions, validate, kind, &numoptions);

    // Return NULL if no options found
    if (numoptions == 0) {
        return NULL;
    }

    // Allocate and fill the structure
    rdopts = allocateReloptStruct(relopt_struct_size, options, numoptions);
    fillRelOptions(rdopts, relopt_struct_size, options, numoptions,
                   validate, relopt_elems, num_relopt_elems);

    // Clean up temporary options array
    pfree(options);

    return rdopts;
}
```