# heap_modify_tuple

## Location
src/backend/access/common/heaptuple.c: 1209 - 1276

## Overview
Creates a new HeapTuple by selectively replacing attributes from an existing tuple with new values based on a replacement mask.

## Definition
```c
HeapTuple heap_modify_tuple(HeapTuple tuple, TupleDesc tupleDesc, const Datum *replValues, const bool *replIsnull, const bool *doReplace)
```

## Detailed Description
This function creates a modified copy of an existing tuple by selectively replacing some attributes while preserving others. The process involves:

1. **Deformation**: Uses `heap_deform_tuple` to extract all attribute values and null flags from the original tuple
2. **Selective replacement**: Iterates through all attributes, replacing values where `doReplace[i]` is true with corresponding values from `replValues` and `replIsnull`
3. **Reformation**: Calls `heap_form_tuple` to construct a new tuple from the combined attribute arrays
4. **Metadata preservation**: Copies identification information (`t_ctid`, `t_self`, `t_tableOid`) from the original tuple to maintain tuple identity

The function uses a linear O(N) approach by deforming the entire tuple upfront rather than selectively calling `heap_getattr` for unchanged columns, which would result in O(N²) complexity.

## Parameters / Member Variables
- `tuple`: The source HeapTuple to be modified
- `tupleDesc`: TupleDesc describing the tuple structure and types
- `replValues`: Array of replacement Datum values (length must equal tupleDesc->natts)
- `replIsnull`: Array of null flags for replacement values (same length as replValues)
- `doReplace`: Boolean array indicating which attributes to replace (same length as replValues)

## Dependencies
- Functions called/Symbols referenced:
  - palloc (for temporary arrays)
  - heap_deform_tuple
  - heap_form_tuple
  - pfree (cleanup of temporary arrays)
- Called from (representative examples):
  - SetDefaultACL
  - ExecGrant_* functions
  - Various ALTER commands (AlterRole, AlterDatabase, etc.)
  - plperl_modify_tuple
  - PLy_modify_tuple

## Notes and Other Information
- Creates a completely new tuple rather than modifying the original in-place
- Preserves tuple identity information (t_ctid, t_self, t_tableOid) from the original
- Uses temporary arrays for values and isnull flags, which are freed after tuple construction
- Commonly used in catalog updates where only certain columns need modification
- The doReplace array allows fine-grained control over which attributes are updated
- Widely used throughout DDL operations and privilege management systems