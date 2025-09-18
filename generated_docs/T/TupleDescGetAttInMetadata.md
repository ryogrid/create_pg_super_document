# TupleDescGetAttInMetadata

## Location
src/backend/executor/execTuples.c: 2173 - 2221

## Overview
TupleDescGetAttInMetadata builds an AttInMetadata structure from a TupleDesc, providing the necessary metadata and function information required to convert C strings into properly formed tuples.

## Definition
AttInMetadata *TupleDescGetAttInMetadata(TupleDesc tupdesc)

## Detailed Description
TupleDescGetAttInMetadata creates an AttInMetadata structure that encapsulates all the information needed to convert text representations of values into PostgreSQL tuple format. The function takes a tuple descriptor and builds corresponding metadata structures that include input functions, I/O parameters, and type modifiers for each attribute.

The process involves several key steps: first, it blesses the tuple descriptor to ensure it can be used for creating rowtype datums; then it iterates through each attribute to gather type-specific input function information using getTypeInputInfo and initializes function manager structures with fmgr_info. The resulting AttInMetadata contains arrays of function pointers and parameters that can be used later to efficiently convert string values to their appropriate PostgreSQL internal representation.

This is particularly useful for functions that need to build tuples from external string data, such as SRFs that process text input or foreign data wrappers that convert external data formats.

## Parameters / Member Variables
- `tupdesc`: A TupleDesc describing the structure and types of the tuple to be created from string data

## Dependencies
- Functions called/Symbols referenced:
  - palloc
  - BlessTupleDesc
  - palloc0
  - TupleDescAttr
  - getTypeInputInfo
  - fmgr_info

- Called from (representative examples):
  - mxact
  - libpqrcv_processTuples
  - pg_stats_ext_mcvlist_items
  - tt_setup_firstcall
  - prs_setup_firstcall
  - pg_get_keywords
  - show_all_settings
  - pltcl_func_handler
  - pltcl_build_tuple_result

## Notes and Other Information
- The function automatically blesses the tuple descriptor to ensure it can be used for rowtype datum creation
- Dropped attributes are ignored during the metadata gathering process
- Memory allocation uses palloc0 to ensure that arrays are zero-initialized, which is important for dropped attributes
- The resulting AttInMetadata structure contains function pointers that are ready to use for string-to-datum conversion
- This is commonly used in SRFs, foreign data wrappers, and other components that need to construct tuples from external text data
- The AttInMetadata structure provides efficient access to type input functions without requiring repeated catalog lookups