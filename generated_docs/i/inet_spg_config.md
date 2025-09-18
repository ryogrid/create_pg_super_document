# inet_spg_config

## Location
[src/backend/utils/adt/network_spgist.c:51-67](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/network_spgist.c#L51-L67)

## Overview
SP-GiST configuration function for inet/cidr data types that defines the index structure and operational parameters.

## Definition
```c
Datum inet_spg_config(PG_FUNCTION_ARGS)
```

## Detailed Description
The `inet_spg_config` function serves as the configuration entry point for SP-GiST (Space-Partitioned Generalized Search Tree) indexing of network address data types (inet and cidr). This function is called during index creation to establish the fundamental parameters that govern how the SP-GiST index will organize and manage network address data.

The function configures the index to use CIDR (Classless Inter-Domain Routing) format as the prefix type, which allows for hierarchical organization of network addresses based on their network prefixes. It disables label storage (setting labelType to VOID) since network address comparison can be performed directly on the data without additional labeling. The configuration enables data return capability, allowing the index to return actual data values directly from leaf nodes, and disables long values support since network addresses have a fixed maximum size.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `cfgin` (unused): Input configuration parameters from the query planner
  - `cfg`: Output configuration structure to be populated with index parameters

## Dependencies
- Functions called/Symbols referenced:
  - [spgConfigOut](../s/spgConfigOut.md) (structure type for output configuration)
  - PG_RETURN_VOID (macro for returning void from PostgreSQL function)
- Called from (representative examples):
  - SP-GiST index creation process
  - Index definition validation

## Notes and Other Information
- This function is part of the SP-GiST operator class implementation for network data types
- The configuration enables the index to store actual data in leaf nodes (canReturnData = true)
- Long values are disabled (longValuesOK = false) because network addresses have bounded size
- The function uses CIDROID as the prefix type, allowing hierarchical network organization
- No custom labeling is used (labelType = VOIDOID) since network addresses can be compared directly