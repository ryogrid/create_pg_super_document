# compute_padlen

## Location
[src/port/snprintf.c:1478-1491](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/port/snprintf.c#L1478-L1491)

## Overview
Calculates the amount of padding needed to achieve a minimum field width for formatted output, taking into account left-justification settings.

## Definition
```c
static int compute_padlen(int minlen, int vallen, int leftjust)
```

## Detailed Description
The `compute_padlen` function is a utility function in PostgreSQL's custom sprintf implementation that computes the padding length required to format a value within a specified minimum field width. The function handles both right-justified (default) and left-justified formatting by returning positive padding values for right-justification and negative values for left-justification.

This function is essential for implementing format specifiers that control field width, such as `%10d` (right-justified in 10 characters) or `%-10s` (left-justified in 10 characters). The sign of the returned value indicates the padding direction: positive for right padding (left-justified content) and negative for left padding (right-justified content).

## Parameters / Member Variables
- `minlen`: The minimum field width specified in the format string
- `vallen`: The actual length of the value being formatted  
- `leftjust`: Flag indicating whether left-justification is requested (non-zero for left-justified)

## Dependencies
- Functions called/Symbols referenced:
  - (None - this is a leaf function)
- Called from (representative examples):
  - [flushbuffer](../f/flushbuffer.md) (at src/port/snprintf.c:335)
  - [fmtstr](../f/fmtstr.md) (at src/port/snprintf.c:979)
  - [fmtint](../f/fmtint.md) (at src/port/snprintf.c:1105)
  - [fmtchar](../f/fmtchar.md) (at src/port/snprintf.c:1122)
  - [fmtfloat](../f/fmtfloat.md) (at src/port/snprintf.c:1234)

## Notes and Other Information
- This is a static function within the snprintf.c module, indicating it's an internal utility
- Returns 0 when no padding is needed (value length >= minimum length)
- Uses negative padding values to indicate left-justification, which simplifies the logic in calling functions
- Part of PostgreSQL's portable snprintf implementation ensuring consistent formatting behavior across platforms
- The function handles the core logic for format width specifiers in printf-family functions
- Always ensures non-negative actual padding by setting padlen to 0 when minlen < vallen

## Simplified Source

```c
static int compute_padlen(int minlen, int vallen, int leftjust) {
    // Calculate padding needed: minlen - actual value length
    int padlen = minlen - vallen;

    // No padding if value is already long enough
    if (padlen < 0)
        padlen = 0;

    // Left-justified: return negative padding (indicates left padding)
    // Right-justified: return positive padding (indicates right padding)
    if (leftjust)
        padlen = -padlen;

    return padlen;
}
```