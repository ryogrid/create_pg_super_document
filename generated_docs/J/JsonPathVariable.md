# JsonPathVariable

## Location
[src/include/utils/jsonpath.h:287-295](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/jsonpath.h#L287-L295)

## Overview
JsonPathVariable represents an external variable that can be passed into and referenced by JSON path expressions during execution, providing a mechanism for parameterized JSON path queries.

## Definition

```c
typedef struct JsonPathVariable
{
	char	   *name;
	int			namelen;		/* strlen(name) as cache for GetJsonPathVar() */
	Oid			typid;
	int32		typmod;
	Datum		value;
	bool		isnull;
} JsonPathVariable;
```
## Detailed Description
JsonPathVariable is a data structure that encapsulates external variables used in JSON path expressions. These variables allow JSON path queries to reference external values by name, enabling parameterized queries and dynamic behavior. The structure stores not only the variable's name and value but also complete type information necessary for proper PostgreSQL type handling. The namelen field serves as a performance optimization by caching the string length to avoid repeated strlen() calls during variable lookups. This structure is essential for implementing variable substitution in JSON path expressions, allowing for flexible and reusable query patterns.

## Parameters / Member Variables
- `*name`: Pointer to the variable name string
- `namelen`: Cached length of the name string (equivalent to strlen(name)) for performance optimization
- `typid`: PostgreSQL type OID identifying the data type of the variable's value
- `typmod`: Type modifier providing additional type-specific information (e.g., precision for numeric types)
- `value`: The actual variable value stored as a PostgreSQL Datum
- `isnull`: Boolean flag indicating whether the variable value is NULL
## Dependencies
- Functions called/Symbols referenced:
  - PostgreSQL type system (Oid, Datum types)
- Called from (representative examples):
  - [ExecInitJsonExpr](../E/ExecInitJsonExpr.md)
  - [GetJsonPathVar](../G/GetJsonPathVar.md)
  - [JsonTableInitOpaque](JsonTableInitOpaque.md)

## Notes and Other Information
- Part of PostgreSQL's SQL/JSON implementation supporting parameterized JSON path queries
- [Variables](../V/Variables.md) are referenced in JSON path expressions using the $ syntax (e.g., $.keyname == )
- The structure includes full PostgreSQL type information enabling proper type coercion and validation
- Used extensively in JSON table functions and JSON query operations where dynamic values are needed
- The namelen caching improves performance during variable name lookups in the JSON path executor