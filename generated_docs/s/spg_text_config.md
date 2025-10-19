# spg_text_config

## Location
[src/backend/access/spgist/spgtextproc.c:96-112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgtextproc.c#L96-L112)

## Overview
The spg_text_config function is a SP-GiST (Space-Partitioned Generalized Search Tree) configuration function for text data types that sets up the operational parameters for text indexing in PostgreSQL's SP-GiST access method.

## Definition

```c
Datum
spg_text_config(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the configuration entry point for SP-GiST text indexes. It initializes an spgConfigOut structure with the appropriate settings for handling text data in SP-GiST indexes. The function configures:
- The prefix type as TEXTOID for storing common prefixes
- The label type as INT2OID for node labels  
- Enables data return capability for covering indexes
- Allows long values with suffixing optimization to handle lengthy text strings

The configuration enables efficient text searching by utilizing prefix-based partitioning where common prefixes are stored in inner nodes and suffixes are stored in leaf nodes.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  -  (unused): Input configuration parameters
  - : Output configuration structure to be populated

## Dependencies
- Functions called/Symbols referenced:
  - [spgConfigOut](spgConfigOut.md) (structure type)
  - PG_RETURN_VOID (macro)
- Called from (representative examples):
  - No direct callers found in the indexed codebase

## Notes and Other Information
- This is a PostgreSQL C function following the PG_FUNCTION_ARGS convention
- The function is typically registered as part of SP-GiST operator class definitions
- The longValuesOK setting is crucial for text data as it enables suffixing compression for long strings
- The canReturnData flag enables covering index functionality where indexed columns can be returned without accessing the heap

## Simplified Source

```c
Datum spg_text_config(PG_FUNCTION_ARGS)
{
    // Get output configuration structure
    spgConfigOut *cfg = (spgConfigOut *) PG_GETARG_POINTER(1);

    // Configure SP-GiST for text data
    cfg->prefixType = TEXTOID;      // Store text prefixes in inner nodes
    cfg->labelType = INT2OID;       // Use 2-byte integers for labels
    cfg->canReturnData = true;      // Support covering indexes
    cfg->longValuesOK = true;       // Handle long strings with suffixing

    PG_RETURN_VOID();
}
```