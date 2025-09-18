# XmlPGetDatum

## Location
src/include/utils/xml.h: 57 - 61

## Overview
XmlPGetDatum is an inline function that converts an xmltype pointer back to a PostgreSQL Datum value for use in the PostgreSQL function call interface.

## Definition


## Detailed Description
This function provides the reverse conversion from DatumGetXmlP, converting an xmltype pointer back into a Datum value. It uses PostgreSQL's PointerGetDatum macro to perform the conversion, allowing XML values to be returned from PostgreSQL functions or passed as arguments through the function call interface. This is essential for integrating XML processing functions with PostgreSQL's type system.

## Parameters / Member Variables
- `X`: A const pointer to an xmltype structure containing XML data

## Dependencies
- Functions called/Symbols referenced:
  - PointerGetDatum (implicit through macro)
  - xmltype
- Called from (representative examples):
  - Currently no direct references found in the codebase

## Notes and Other Information
- This is a static inline function defined in src/include/utils/xml.h
- Provides the inverse operation to DatumGetXmlP for converting xmltype back to Datum
- Essential for PostgreSQL's function call interface when returning XML values
- The function accepts a const pointer, indicating it does not modify the XML data
- Part of the standard pattern for PostgreSQL data type conversion functions