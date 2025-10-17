# xml_out

## Location
[src/backend/utils/adt/xml.c:356-370](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/xml.c#L356-L370)

## Overview
PostgreSQL output function that converts the internal xml data type to a C string representation for client communication, specifically removing encoding declarations.

## Definition

```c
Datum
xml_out(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the output conversion function for PostgreSQL's xml data type. It acts as a wrapper around xml_out_internal, specifically designed to handle client communication scenarios. The key feature of this function is that it deliberately removes encoding declarations from the XML output by passing 0 as the target encoding. This design decision prevents potential encoding conflicts that could arise when the output is later converted to different client encodings, ensuring safer XML transmission to clients.

## Parameters / Member Variables
- Takes PostgreSQL function arguments via  macro:
  - Argument 0: xmltype value to be converted to string representation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_XML_P
  - [xml_out_internal](xml_out_internal.md)
  - PG_RETURN_CSTRING
- Called from (representative examples):
  - PostgreSQL's type input/output system (indirectly via function registry)

## Notes and Other Information
- This is a PostgreSQL I/O function, typically registered in the system catalogs as the output function for the xml type
- The function intentionally removes encoding properties to avoid encoding conflicts during client communication
- Uses encoding value 0 to signal xml_out_internal to omit encoding declarations
- Designed for scenarios where PostgreSQL cannot control the final client encoding conversion
- Memory management: relies on xml_out_internal for string allocation, PostgreSQL's memory context handles cleanup
- Part of PostgreSQL's type system infrastructure for converting internal representations to external string formats

## Simplified Source

```c
Datum xml_out(PG_FUNCTION_ARGS) {
    xmltype *x = PG_GETARG_XML_P(0);

    // Remove encoding property to prevent client encoding conflicts
    PG_RETURN_CSTRING(xml_out_internal(x, 0));
}
```