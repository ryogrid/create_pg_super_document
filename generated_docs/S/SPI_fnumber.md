# SPI_fnumber

## Location
src/backend/executor/spi.c: 1175 - 1197

## Overview
Looks up the attribute number (column number) of a named attribute in a tuple descriptor, supporting both regular attributes and system attributes.

## Definition


## Detailed Description
SPI_fnumber searches through a tuple descriptor to find the attribute number corresponding to a given attribute name. It first searches through regular user-defined attributes in the tuple descriptor, then falls back to checking system attributes if no match is found among regular attributes. The function returns a 1-based attribute number for regular attributes (1 to natts) or the actual attribute number for system attributes (which are negative values).

This function is essential for dynamic attribute access in stored procedures and trigger functions where attribute positions may not be known at compile time. It handles dropped attributes correctly by skipping them during the search.

## Parameters / Member Variables
- : The TupleDesc structure containing attribute information for the tuple
- : The name of the attribute to look up (null-terminated string)

## Dependencies
- Functions called/Symbols referenced:
  - TupleDescAttr (macro for accessing tuple descriptor attributes)
  - namestrcmp (for comparing attribute names)
  - [SystemAttributeByName](SystemAttributeByName.md) (for looking up system attributes)
- Called from (representative examples):
  - [make_ruledef](../m/make_ruledef.md) (rule utilities)
  - [make_viewdef](../m/make_viewdef.md) (view definition utilities)
  - [tsvector_update_trigger](../t/tsvector_update_trigger.md) (text search triggers)
  - [plperl_build_tuple_result](../p/plperl_build_tuple_result.md) (Perl procedural language)
  - [PLy_modify_tuple](../P/PLy_modify_tuple.md) (Python procedural language)
  - pltcl_build_tuple_result (Tcl procedural language)
  - [ttdummy](../t/ttdummy.md) (regression test trigger)

## Notes and Other Information
- Returns 1-based attribute numbers for regular attributes (1, 2, 3, ...)
- Returns negative attribute numbers for system attributes (e.g., -1 for ctid, -2 for oid)
- Returns SPI_ERROR_NOATTRIBUTE if the attribute name is not found
- Skips dropped attributes (attisdropped = true) during the search
- System attributes include ctid, oid, xmin, xmax, cmin, cmax, tableoid
- Case-sensitive name matching using namestrcmp()
- Commonly used in procedural languages for dynamic column access
- Essential for generic trigger functions that work with any table structure