# jbvDatetime

## Location
[src/include/utils/jsonb.h:244-252](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/jsonb.h#L244-L252)

## Overview
jbvDatetime is a virtual JSON value type constant in PostgreSQL's JSONB implementation, representing datetime values that exist only during in-memory JSON processing and are serialized to JSON strings on output.

## Definition

```c
struct Jsonb) jbvArray/jbvObject */
	jbvBinary,

	/*
	 * Virtual types.
	 *
	 * These types are used only for in-memory JSON processing and serialized
	 * into JSON strings when outputted to json/jsonb.
	 */
	jbvDatetime = 0x20,
};
```
## Detailed Description
jbvDatetime is an enum constant within the jbvType enumeration that represents datetime values in PostgreSQL's JSONB in-memory processing system. Unlike scalar types (jbvNull, jbvString, jbvNumeric, jbvBool) and composite types (jbvArray, jbvObject, jbvBinary), jbvDatetime is classified as a "virtual type". 

Virtual types like jbvDatetime are special because they:
- Exist only during in-memory JSON manipulation and processing
- Are automatically serialized into JSON strings when the data is output to json/jsonb format
- Allow PostgreSQL to handle datetime values natively during JSON path operations and transformations while maintaining JSON compatibility

The hexadecimal value 0x20 places jbvDatetime in a distinct range from other type constants, clearly identifying it as a virtual type category.

## Parameters / Member Variables
- Value: 0x20 (hexadecimal constant distinguishing it from other jbvType values)

## Dependencies
- Functions called/Symbols referenced:
  - Part of jbvType enum in jsonb.h
- Called from (representative examples):
  - ExecGetJsonValueItemString (src/backend/executor/execExprInterp.c:4509)
  - JsonbTypeName (src/backend/utils/adt/jsonb.c:198)
  - compareJsonbContainers (src/backend/utils/adt/jsonb_util.c:259)
  - convertJsonbScalar (src/backend/utils/adt/jsonb_util.c:1852)
  - executeItemOptUnwrapTarget (src/backend/utils/adt/jsonpath_exec.c:1631)
  - executeDateTimeMethod (src/backend/utils/adt/jsonpath_exec.c:2787)
  - JsonItemFromDatum (src/backend/utils/adt/jsonpath_exec.c:3086)
  - compareItems (src/backend/utils/adt/jsonpath_exec.c:3382)
  - IsAJsonbScalar (src/include/utils/jsonb.h:299)

## Notes and Other Information
- jbvDatetime enables PostgreSQL to handle datetime operations within JSON path expressions while maintaining JSON format compatibility
- The virtual type approach allows PostgreSQL to extend JSON functionality beyond standard JSON types without breaking JSON standards
- When processed through jsonb output functions, jbvDatetime values are automatically converted to their string representation
- This type is particularly important for JSON path operations that involve datetime functions and comparisons