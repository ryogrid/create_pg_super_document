# pair_encode

## Location
[src/backend/utils/adt/geo_ops.c:255-265](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L255-L265)

## Overview
A helper function that converts a coordinate pair (x,y) to its comma-separated string representation for output formatting of geometric data types.

## Definition
```c
static void
pair_encode(float8 x, float8 y, StringInfo str)
```

## Detailed Description
The `pair_encode` function converts two floating-point coordinates to their string representation in comma-separated format "x,y". It serves as a fundamental building block for formatting geometric data types in PostgreSQL output functions. The function converts both coordinates to strings using `float8out_internal`, formats them with a comma separator, appends the result to the provided StringInfo buffer, and properly manages memory by freeing the temporary strings.

This function is essential to PostgreSQL's geometric data type output infrastructure, ensuring consistent coordinate pair formatting across points, paths, circles, and other geometric objects.

## Parameters / Member Variables
- `x`: The first coordinate (x-value) to be converted to string representation
- `y`: The second coordinate (y-value) to be converted to string representation  
- `str`: StringInfo buffer where the formatted coordinate pair will be appended

## Dependencies
- Functions called/Symbols referenced:
  - `[float8out_internal](../f/float8out_internal.md)`: Core function for converting float8 values to strings (called twice)
  - `[appendStringInfo](../a/appendStringInfo.md)`: Function to append formatted string to StringInfo buffer
  - `[pfree](pfree.md)`: Memory management function to free temporary strings (called twice)
- Called from (representative examples):
  - `[path_encode](path_encode.md)`: For formatting individual coordinate pairs within path output
  - `[circle_out](../c/circle_out.md)`: For formatting circle center coordinates in output
  - Used in PATH_CLOSED context for formatting path coordinates

## Notes and Other Information
- This is a static function, only accessible within the geo_ops.c file
- The function handles memory management by freeing both temporary strings returned by float8out_internal
- Part of PostgreSQL's comprehensive geometric data type system output formatting
- Ensures consistent coordinate pair representation across different geometric data types
- Uses StringInfo for efficient string building, which is PostgreSQL's preferred method for constructing output strings
- Produces comma-separated format without parentheses, suitable for embedding in larger geometric constructs
- The comma separator matches PostgreSQL's standard geometric data representation format

## Simplified Source

```c
static void pair_encode(float8 x, float8 y, StringInfo str) {
    // Convert coordinates to string representations
    char *xstr = float8out_internal(x);
    char *ystr = float8out_internal(y);

    // Format as comma-separated pair and append to buffer
    appendStringInfo(str, "%s,%s", xstr, ystr);

    // Free temporary strings
    pfree(xstr);
    pfree(ystr);
}
```