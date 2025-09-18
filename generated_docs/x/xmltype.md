# xmltype

## Location
src/include/utils/xml.h: 23 - 26

## Overview
xmltype is a typedef that defines the PostgreSQL data type structure for storing XML values, based on the variable-length varlena structure.

## Definition


## Detailed Description
The xmltype is PostgreSQL's internal representation for XML data values. It is defined as a typedef of the varlena structure, which is the standard header used by all variable-length data types in PostgreSQL. This structure supports PostgreSQL's TOAST (The Oversized-Attribute Storage Technique) mechanism, allowing XML values to be compressed or stored out-of-line when they exceed certain size thresholds. The xmltype provides the foundation for all XML operations in PostgreSQL, including parsing, validation, manipulation, and storage.

## Parameters / Member Variables
As a varlena structure, xmltype contains:
- `vl_len_[4]`: 4-byte length field (should not be accessed directly, use VARSIZE macros instead)
- `vl_dat[]`: Flexible array member containing the actual XML data content

## Dependencies
- Functions called/Symbols referenced:
  - [varlena](../v/varlena.md) (base structure)
- Called from (representative examples):
  - [xml_in](xml_in.md), xml_out, xml_recv, xml_send (I/O functions)
  - [xmlconcat](xmlconcat.md), xmlelement, xmlparse, xmlpi, xmlroot (XML manipulation functions)
  - [xpath](xpath.md), xmlexists, xpath_exists (XPath functions)
  - [DatumGetXmlP](../D/DatumGetXmlP.md), XmlPGetDatum (conversion functions)
  - [ExecEvalXmlExpr](../E/ExecEvalXmlExpr.md) (expression evaluation)

## Notes and Other Information
- Defined in src/include/utils/xml.h at line 23
- Based on PostgreSQL's standard varlena structure for variable-length types
- Supports TOAST compression and out-of-line storage for large XML documents
- Always use VARDATA, VARSIZE, and related macros instead of direct field access
- Extensively used throughout PostgreSQL's XML subsystem in src/backend/utils/adt/xml.c
- The actual XML content is stored in the vl_dat array member following the 4-byte length header