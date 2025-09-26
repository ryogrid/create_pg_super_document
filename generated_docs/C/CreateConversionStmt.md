# CreateConversionStmt

## Location
src/include/nodes/parsenodes.h: 3988 - 3996

## Overview
CreateConversionStmt represents a CREATE CONVERSION statement in PostgreSQL's parse tree, which is used to define a new encoding conversion function for character set conversion.

## Definition
```c
typedef struct CreateConversionStmt
{
    NodeTag     type;
    List       *conversion_name;      /* Name of the conversion */
    char       *for_encoding_name;    /* source encoding name */
    char       *to_encoding_name;     /* destination encoding name */
    List       *func_name;            /* qualified conversion function name */
    bool        def;                  /* is this a default conversion? */
} CreateConversionStmt;
```

## Detailed Description
CreateConversionStmt is a parse tree node that represents the CREATE CONVERSION SQL command. This statement creates a new conversion between two character encodings, specifying a function that performs the actual conversion. Conversions are essential for PostgreSQL's multi-encoding support, allowing data to be converted between different character sets when clients and servers use different encodings.

The conversion can be marked as default, meaning it will be automatically used when conversion between the specified encodings is needed. The conversion function must follow specific signature requirements and handle the conversion logic between the source and destination encodings.

## Parameters / Member Variables
- `type`: Standard NodeTag identifying this as a CreateConversionStmt node in the parse tree
- `conversion_name`: List representing the qualified name of the conversion (schema.conversion_name)
- `for_encoding_name`: String specifying the source character encoding name
- `to_encoding_name`: String specifying the destination character encoding name
- `func_name`: List representing the qualified name of the conversion function
- `def`: Boolean flag indicating whether this should be the default conversion for these encodings

## Dependencies
- Functions called/Symbols referenced:
  - List (PostgreSQL's list data structure)
  
- Called from (representative examples):
  - CreateConversionCommand (main execution function in conversioncmds.c:32)
  - ProcessUtilitySlow (utility command processor in utility.c:1716)

## Notes and Other Information
- The conversion function must have a specific signature: (src_encoding int4, dest_encoding int4, src cstring, dest internal, len int4, noError bool) returns void
- Conversions to or from SQL_ASCII are generally not permitted as they are considered meaningless
- Only one default conversion can exist between any pair of encodings
- The conversion function is responsible for proper character set transformation and error handling
- Requires CREATE privilege on the target schema
- The conversion is stored in the system catalog pg_conversion
- Used primarily for internationalization and multi-language database support
- Conversion functions typically use external libraries like ICU for proper character set handling