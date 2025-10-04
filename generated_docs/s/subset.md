# subset

## Location
[src/backend/regex/regexec.c:702-755](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regexec.c#L702-L755)

## Overview
Sets subexpression match data for a successful subre by recording the start and end positions of the matched text.

## Definition
static void subset(struct vars *v, struct subre *sub, chr *begin, chr *end)

## Detailed Description
The subset function is responsible for recording successful subexpression matches in PostgreSQL's regex execution engine. When a subexpression (capture group) successfully matches a portion of the input text, this function stores the match boundaries in the pmatch array within the vars structure.

The function takes the beginning and ending character pointers of the matched text and converts them to offsets using the OFF macro, which are then stored in the rm_so (start offset) and rm_eo (end offset) fields of the corresponding regmatch_t structure. The function includes bounds checking to ensure the capture number is within the valid range of the pmatch array.

Debug information is also logged when MDEBUG is enabled, showing the subexpression ID, capture number, and the calculated offsets.

## Parameters / Member Variables
- v: Pointer to a vars structure containing regex execution state and match results
- sub: Pointer to a subre structure representing the subexpression that matched
- begin: Pointer to the first character of the matched text
- end: Pointer to the character after the last character of the matched text

## Dependencies
- Functions called/Symbols referenced:
  - struct vars (regex execution variables structure)
  - struct subre (subexpression tree node structure)
  - [chr](../c/chr.md) (character type)
  - LOFF (macro for converting pointer to offset for debugging)
  - MDEBUG (debug logging macro)
  - OFF (macro for converting character pointer to offset)
- Called from (representative examples):
  - LOFF (macro/function at src/backend/regex/regexec.c:148)
  - [cdissect](../c/cdissect.md) (function at src/backend/regex/regexec.c:820)

## Notes and Other Information
- This is a static function, only accessible within regexec.c
- Requires that the capture number (sub->capno) is greater than 0
- Performs bounds checking to prevent array access violations
- Uses assert() to validate that the capture number is positive
- The function only records successful matches; failed matches are handled elsewhere
- Part of PostgreSQL's POSIX-compliant regex implementation
- The OFF macro handles the conversion from character pointers to integer offsets
- Debug output includes the subexpression ID for troubleshooting complex patterns

## Simplified Source

```c
static void subset(struct vars *v, struct subre *sub, chr *begin, chr *end) {
    int n = sub->capno;

    // Only process valid capture numbers
    assert(n > 0);
    if ((size_t) n >= v->nmatch)
        return;

    // Store match boundaries as offsets
    v->pmatch[n].rm_so = OFF(begin);
    v->pmatch[n].rm_eo = OFF(end);
}
```