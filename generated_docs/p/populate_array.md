# populate_array

## Location
[src/backend/utils/adt/jsonfuncs.c:2913-2979](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonfuncs.c#L2913-L2979)

## Overview
The main entry point function that converts JSON or JSONB values into PostgreSQL multi-dimensional array structures.

## Definition

```c
static Datum
populate_array(ArrayIOData *aio,
			   const char *colname,
			   MemoryContext mcxt,
			   JsValue *jsv,
			   bool *isnull,
			   Node *escontext)
```
## Detailed Description
This function serves as the primary interface for converting JSON or JSONB data into PostgreSQL arrays. It initializes a PopulateArrayContext with the necessary array metadata and memory contexts, then dispatches to the appropriate parsing function based on whether the input is JSON text or binary JSONB. The function handles both text JSON (via populate_array_json) and binary JSONB (via populate_array_dim_jsonb) formats. After successful parsing, it constructs the final multi-dimensional array using PostgreSQL's array building infrastructure, setting appropriate lower bounds and managing memory cleanup.

## Parameters / Member Variables
- : ArrayIOData pointer containing array type information and I/O functions
- : Character pointer to the column name for error reporting purposes
- : MemoryContext for array element allocations during parsing
- : JsValue pointer containing either JSON text or JSONB binary data to convert
- : Boolean pointer set to true if parsing errors occur, false on success
- : Node pointer for error context and soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - [ArrayIOData](../A/ArrayIOData.md) (array metadata structure)
  - [JsValue](../J/JsValue.md) (JSON/JSONB value wrapper)
  - [PopulateArrayContext](../P/PopulateArrayContext.md) (parsing context structure)
  - [initArrayResult](../i/initArrayResult.md) (array building state initialization)
  - [populate_array_json](populate_array_json.md) (JSON text parsing)
  - [populate_array_dim_jsonb](populate_array_dim_jsonb.md) (JSONB binary parsing)
  - [makeMdArrayResult](../m/makeMdArrayResult.md) (multi-dimensional array construction)
- Called from (representative examples):
  - JsObjectFree
  - [populate_record_field](populate_record_field.md)

## Notes and Other Information
- This is a static function within jsonfuncs.c, serving as an internal implementation detail
- Returns a Datum representing the constructed PostgreSQL array, or (Datum) 0 on error
- The function handles memory management by using separate contexts for different allocation lifetimes
- Lower bounds for all dimensions are set to 1, following PostgreSQL array conventions
- Supports error-safe operation through the escontext parameter, allowing callers to handle errors gracefully
- The function assumes that dimension information will be determined during the parsing process
- Memory cleanup is performed for temporary allocations (dims, sizes, lbs arrays) but not for the final array result
- Part of PostgreSQL's JSON-to-native-type conversion infrastructure, enabling seamless integration between JSON data and PostgreSQL's type system