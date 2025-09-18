# toast_tuple_init

## Location
[src/backend/access/table/toast_helper.c:41-180](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/table/toast_helper.c#L41-L180)

## Overview
Initializes a TOAST tuple context structure to prepare for toasting operations on a tuple, setting up attribute flags and handling external values from existing tuples during updates.

## Definition


## Detailed Description
This function prepares the ToastTupleContext structure for tuple toasting operations. It analyzes each attribute in the tuple to determine what toasting actions are needed. For new tuples (INSERT), it simply examines the new values. For updates (UPDATE), it compares old and new values to determine which external values need cleanup and which can be reused.

The function iterates through all attributes in the tuple descriptor and:
- Initializes per-attribute flags and metadata
- For updates, compares old and new external values to determine if cleanup is needed
- Handles NULL attributes appropriately
- Processes varlena attributes and sets up proper storage strategy
- Fetches external values that cannot be reused
- Sets various flags to indicate what operations will be needed during toasting

## Parameters / Member Variables
- : ToastTupleContext structure containing:
  - : Relation descriptor
  - : Array of new attribute values
  - : Array of NULL flags for new values
  - : Array of old attribute values (NULL for INSERT)
  - : Array of NULL flags for old values (NULL for INSERT)
  - : Output array of per-attribute toast information
  - : Output flags indicating needed operations

## Dependencies
- Functions called/Symbols referenced:
  - TupleDescAttr
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - VARATT_IS_EXTERNAL_ONDISK
  - VARSIZE_EXTERNAL
  - VARATT_IS_EXTERNAL
  - [detoast_attr](../d/detoast_attr.md)
  - [detoast_external_attr](../d/detoast_external_attr.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - VARSIZE_ANY
- Called from (representative examples):
  - [heap_toast_insert_or_update](../h/heap_toast_insert_or_update.md)

## Notes and Other Information
- This is the first step in the tuple toasting process, setting up the context for subsequent compression and externalization operations
- The function carefully handles UPDATE scenarios by comparing old and new values to avoid unnecessary work
- External values that haven't changed can be reused, avoiding the need to re-externalize them
- Sets up flags that guide later stages of the toasting process
- Part of PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) system for handling large attribute values