# spgist_name_config

## Location
[src/test/modules/spgist_name_ops/spgist_name_ops.c:34-51](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/spgist_name_ops/spgist_name_ops.c#L34-L51)

## Overview
A configuration function for the SP-GiST (Space-Partitioned Generalized Search Tree) operator class designed to handle PostgreSQL 'name' data types.

## Definition

```c
Datum
spgist_name_config(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the configuration entry point for an SP-GiST operator class that handles PostgreSQL 'name' data types. It sets up the necessary configuration parameters for the SP-GiST index structure, defining the data types used for different components of the index (prefix, label, and leaf nodes) and enabling specific optimizations.

The function configures the SP-GiST to:
- Use TEXT data type for both prefix and leaf storage
- Use INT2 (smallint) for labels  
- Enable data return capability for covering indexes
- Allow handling of long values through suffixing mechanism

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro, containing:
  -  (spgConfigIn*): Input configuration (commented out, not used)
  -  (spgConfigOut*): Output configuration structure to be populated

## Dependencies
- Functions called/Symbols referenced:
  - [spgConfigOut](spgConfigOut.md)
  - PG_RETURN_VOID
- Called from (representative examples):
  - No direct callers found (registered as SP-GiST config function)

## Notes and Other Information
- Located in src/test/modules/spgist_name_ops/spgist_name_ops.c:34-51
- This is part of a test module demonstrating SP-GiST operator class implementation
- The suffixing mechanism (longValuesOK = true) allows the index to handle values longer than the page size by storing partial data
- The canReturnData setting enables covering index functionality where indexed values can be returned without accessing the heap

## Simplified Source

```c
Datum
spgist_name_config(PG_FUNCTION_ARGS)
{
    spgConfigOut *cfg = (spgConfigOut *) PG_GETARG_POINTER(1);

    // Configure SP-GiST index structure
    cfg->prefixType = TEXTOID;        // Prefix type: TEXT
    cfg->labelType = INT2OID;         // Label type: smallint
    cfg->leafType = TEXTOID;          // Leaf type: TEXT
    cfg->canReturnData = true;        // Enable covering indexes
    cfg->longValuesOK = true;         // Support long values via suffixing

    PG_RETURN_VOID();
}
```