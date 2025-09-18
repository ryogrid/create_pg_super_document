# spg_range_quad_config

## Location
src/backend/utils/adt/rangetypes_spgist.c: 60 - 94

## Overview
SP-GiST configuration function for range type quadtree indexing that defines the structural parameters and capabilities of the index.

## Definition


## Detailed Description
This function serves as the configuration interface for SP-GiST quadtree indexing of range types. It initializes the spgConfigOut structure to define how the SP-GiST index should be structured for range data. The function configures the index to use ANYRANGEOID as the prefix type (allowing the centroid range to be stored), sets no node labels (VOIDOID), enables data return capability, and disallows long values to maintain efficient quadtree operations.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro for PostgreSQL function argument handling
- : Input configuration parameter (commented out as unused)
- : Output configuration structure that gets populated with index settings

## Dependencies
- Functions called/Symbols referenced:
  - spgConfigOut (structure type)
  - PG_RETURN_VOID (macro)
- Called from (representative examples):
  - Used as part of SP-GiST operator class registration for range types

## Notes and Other Information
- Sets prefixType to ANYRANGEOID to allow storing range centroids as prefixes
- Uses VOIDOID for labelType since node labels are not needed in quadtree structure
- Enables canReturnData to support index-only scans when possible
- Disables longValuesOK to ensure quadtree efficiency with range data
- Located in src/backend/utils/adt/rangetypes_spgist.c:60-94