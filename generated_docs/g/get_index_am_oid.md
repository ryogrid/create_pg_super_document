# get_index_am_oid

## Location
src/backend/commands/amcmds.c: 163 - 172

## Overview
Looks up an access method by name and verifies it corresponds to an index access method, returning its OID.

## Definition


## Detailed Description
get_index_am_oid is a specialized wrapper function that provides type-safe lookup of index access methods. It leverages the internal get_am_type_oid function with the AMTYPE_INDEX constraint to ensure that only valid index access methods are returned. This function is commonly used throughout the system when creating or manipulating index-related objects that require validation of the access method type.

## Parameters / Member Variables
- : Name of the index access method to look up
- : If false, throws error when access method not found; if true, returns InvalidOid

## Dependencies
- Functions called/Symbols referenced:
  - get_am_type_oid: Internal worker function for access method lookup
  - AMTYPE_INDEX: Constant defining the index access method type
- Called from (representative examples):
  - get_object_address_opcf: Object address resolution for operator classes/families
  - DefineOpFamily: Operator family definition processing
  - transformIndexConstraint: Index constraint transformation in parser

## Notes and Other Information
- Provides type-safe interface specifically for index access methods
- Thin wrapper around get_am_type_oid with AMTYPE_INDEX constraint
- Used extensively in index creation and manipulation operations
- Ensures that only index-compatible access methods are accepted
- Location: src/backend/commands/amcmds.c:163-172