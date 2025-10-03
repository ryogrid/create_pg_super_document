# JsonbToCStringWorker

## Location
[src/backend/utils/adt/jsonb.c:491-614](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/jsonb.c#L491-L614)

## Overview
The core static function that performs the actual conversion of JSONB containers to string representation, with optional indentation support for both compact and pretty-printed output.

## Definition

```c
static char *
JsonbToCStringWorker(StringInfo out, JsonbContainer *in, int estimated_len, bool indent)
```
## Detailed Description
This function is the heart of JSONB to string conversion in PostgreSQL. It uses a JsonbIterator to traverse the JSONB structure and builds the string representation token by token. The function handles all JSONB types including objects, arrays, and scalar values, with support for both compact and indented output formats. It manages proper JSON syntax including commas, brackets, braces, colons, and indentation. The function includes special handling for raw scalar values (top-level scalars wrapped in arrays) and maintains proper nesting levels for indentation. It uses an iterator-based approach that allows for efficient memory usage and streaming output generation.

## Parameters / Member Variables
- `out`: Optional StringInfo buffer for output; if NULL, a new buffer is created
- `*in`: Pointer to the JsonbContainer structure containing the JSONB data to convert
- `estimated_len`: Estimated length for buffer pre-allocation (uses 64 as default if negative)
- `indent`: Boolean flag controlling whether to generate pretty-printed output with indentation
## Dependencies
- Functions called/Symbols referenced:
  - [makeStringInfo](../m/makeStringInfo.md), enlargeStringInfo (for buffer management)
  - [JsonbIteratorInit](JsonbIteratorInit.md), JsonbIteratorNext (for JSONB traversal)
  - [jsonb_put_escaped_value](../j/jsonb_put_escaped_value.md) (for scalar value conversion)
  - [add_indent](../a/add_indent.md) (for indentation formatting)
  - [appendBinaryStringInfo](../a/appendBinaryStringInfo.md), appendStringInfoCharMacro (for string building)
  - JsonbIterator, JsonbIteratorToken, JsonbValue (JSONB iterator types)
  - WJB_* constants (iterator token types: WJB_DONE, WJB_BEGIN_ARRAY, WJB_BEGIN_OBJECT, WJB_KEY, WJB_ELEM, WJB_VALUE, WJB_END_ARRAY, WJB_END_OBJECT)
- Called from (representative examples):
  - [JsonbToCString](JsonbToCString.md) (with indent=false)
  - [JsonbToCStringIndent](JsonbToCStringIndent.md) (with indent=true)

## Notes and Other Information
- This is a static function used internally by the JSONB string conversion functions
- Uses a state machine approach with redo_switch logic to handle complex iterator sequences
- Manages multiple state variables: first (comma handling), level (nesting depth), use_indent (indentation control), raw_scalar (top-level scalar detection), last_was_key (formatting context)
- The ispaces variable controls spacing: 1 space after comma when indenting, 2 when not
- Special handling for raw scalars (top-level scalars) where array brackets are omitted
- Includes assertions to ensure proper nesting (level == 0 at end)
- Efficiently handles both object key-value pairs and array elements
- The function is designed to be memory-efficient and can work with very large JSONB structures
- Returns the data pointer from the StringInfo buffer, making the caller responsible for memory management when out was NULL

## Simplified Source

```c
static char *JsonbToCStringWorker(StringInfo out, JsonbContainer *in, int estimated_len, bool indent) {
    bool first = true;
    JsonbIterator *it;
    JsonbValue v;
    JsonbIteratorToken type;
    int level = 0;
    bool raw_scalar = false;
    bool last_was_key = false;

    // Initialize output buffer
    if (out == NULL)
        out = makeStringInfo();
    enlargeStringInfo(out, (estimated_len >= 0) ? estimated_len : 64);

    // Initialize iterator for JSONB traversal
    it = JsonbIteratorInit(in);

    // Main processing loop
    while ((type = JsonbIteratorNext(&it, &v, false)) != WJB_DONE) {
        switch (type) {
            case WJB_BEGIN_ARRAY:
                if (!first) appendBinaryStringInfo(out, ", ", indent ? 1 : 2);

                if (!v.val.array.rawScalar) {
                    add_indent(out, indent && !last_was_key, level);
                    appendStringInfoCharMacro(out, '[');
                } else {
                    raw_scalar = true;  // Top-level scalar wrapped in array
                }
                first = true;
                level++;
                break;

            case WJB_BEGIN_OBJECT:
                if (!first) appendBinaryStringInfo(out, ", ", indent ? 1 : 2);
                add_indent(out, indent && !last_was_key, level);
                appendStringInfoCharMacro(out, '{');
                first = true;
                level++;
                break;

            case WJB_KEY:
                if (!first) appendBinaryStringInfo(out, ", ", indent ? 1 : 2);
                first = true;
                add_indent(out, indent, level);

                // Output key and get corresponding value
                jsonb_put_escaped_value(out, &v);
                appendBinaryStringInfo(out, ": ", 2);

                type = JsonbIteratorNext(&it, &v, false);
                if (type == WJB_VALUE) {
                    first = false;
                    jsonb_put_escaped_value(out, &v);
                } else {
                    // Value is a nested container, will be handled in next iteration
                    continue;
                }
                break;

            case WJB_ELEM:
                if (!first) appendBinaryStringInfo(out, ", ", indent ? 1 : 2);
                first = false;
                if (!raw_scalar) add_indent(out, indent, level);
                jsonb_put_escaped_value(out, &v);
                break;

            case WJB_END_ARRAY:
                level--;
                if (!raw_scalar) {
                    add_indent(out, indent, level);
                    appendStringInfoCharMacro(out, ']');
                }
                first = false;
                break;

            case WJB_END_OBJECT:
                level--;
                add_indent(out, indent, level);
                appendStringInfoCharMacro(out, '}');
                first = false;
                break;

            default:
                elog(ERROR, "unknown jsonb iterator token type");
        }
        last_was_key = (type == WJB_KEY);
    }

    return out->data;
}
```