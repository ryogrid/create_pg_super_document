# cclass_column_index

## Location
[src/backend/regex/regc_locale.c:671-716](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_locale.c#L671-L716)

## Overview
The cclass_column_index function computes a column index for the high colormap based on which character classes a given character belongs to, used in PostgreSQL's regex color mapping system.

## Definition

```c
struct colormap *cm, chr c)
{
	int			colnum = 0;

	/* Shouldn't go through all these pushups for simple chrs */
	assert(c > MAX_SIMPLE_CHR);

	/*
	 * Note: we should not see requests to consider cclasses that are not
	 * treated as locale-specific by cclasscvec(), above.
	 */
	if (cm->classbits[CC_PRINT] && pg_wc_isprint(c))
		colnum |= cm->classbits[CC_PRINT];
	if (cm->classbits[CC_ALNUM] && pg_wc_isalnum(c))
		colnum |= cm->classbits[CC_ALNUM];
	if (cm->classbits[CC_ALPHA] && pg_wc_isalpha(c))
		colnum |= cm->classbits[CC_ALPHA];
	if (cm->classbits[CC_WORD] && pg_wc_isword(c))
		colnum |= cm->classbits[CC_WORD];
	assert(cm->classbits[CC_ASCII] == 0);
	assert(cm->classbits[CC_BLANK] == 0);
	assert(cm->classbits[CC_CNTRL] == 0);
	if (cm->classbits[CC_DIGIT] && pg_wc_isdigit(c))
		colnum |= cm->classbits[CC_DIGIT];
	if (cm->classbits[CC_PUNCT] && pg_wc_ispunct(c))
		colnum |= cm->classbits[CC_PUNCT];
	assert(cm->classbits[CC_XDIGIT] == 0);
	if (cm->classbits[CC_SPACE] && pg_wc_isspace(c))
		colnum |= cm->classbits[CC_SPACE];
	if (cm->classbits[CC_LOWER] && pg_wc_islower(c))
		colnum |= cm->classbits[CC_LOWER];
	if (cm->classbits[CC_UPPER] && pg_wc_isupper(c))
		colnum |= cm->classbits[CC_UPPER];
	if (cm->classbits[CC_GRAPH] && pg_wc_isgraph(c))
		colnum |= cm->classbits[CC_GRAPH];

	return colnum;
}

/*
 * allcases - supply cvec for all case counterparts of a chr (including itself)
 *
 * This is a shortcut, preferably an efficient one, for simple characters;
```
## Detailed Description
The cclass_column_index function is a critical component of PostgreSQL's regex colormap optimization system. It determines the appropriate column index within the high-resolution colormap array by evaluating which character classes the given character belongs to.

The function works by:

1. **Character class evaluation**: It tests the character against all locale-dependent character classes (those handled by pg_wc_* functions) that are marked as active in the colormap's classbits array.

2. **Bitwise combination**: For each character class the character belongs to, it ORs the corresponding bit value from classbits into the result, creating a unique index that represents the combination of character class memberships.

3. **Locale-specific optimization**: Only processes character classes that are locale-dependent (those using pg_wc_* functions), as indicated by the assertions that hard-wired classes (ASCII, BLANK, CNTRL, XDIGIT) have zero classbits.

This function enables the regex engine to efficiently map characters to colors based on their character class memberships, which is essential for regex optimization. Characters with identical class memberships can share the same color, reducing the complexity of the finite automaton.

The function is only called for characters above MAX_SIMPLE_CHR, as simpler characters are handled through direct lookup mechanisms.

## Parameters

- `cm`: Pointer to the colormap structure containing class bit assignments and colormap configuration
- `c`: The character for which to compute the column index (must be > MAX_SIMPLE_CHR)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_wc_isprint](../p/pg_wc_isprint.md), pg_wc_isalnum, pg_wc_isalpha, pg_wc_isword (character classification)
  - [pg_wc_isdigit](../p/pg_wc_isdigit.md), pg_wc_ispunct, pg_wc_isspace (character classification)  
  - [pg_wc_islower](../p/pg_wc_islower.md), pg_wc_isupper, pg_wc_isgraph (character classification)
  - CC_* constants (character class identifiers)
  - MAX_SIMPLE_CHR (threshold for simple character handling)
- Called from (representative examples):
  - [pg_reg_getcolor](../p/pg_reg_getcolor.md) (in regc_color.c:158 for colormap lookup)

## Notes and Other Information
- Only processes locale-dependent character classes; hard-wired classes are asserted to have zero classbits
- Returns a bitwise combination of active character class memberships as the column index
- Part of PostgreSQL's regex colormap optimization that groups characters by class membership
- Must be kept synchronized with the cclasscvec() function's character class handling
- Input character must be above MAX_SIMPLE_CHR threshold for this function to be called
- The returned index is used to access specific columns in the high-resolution colormap array

## Simplified Source

```c
static int
cclass_column_index(struct colormap *cm, chr c)
{
    int colnum = 0;

    // Only handle complex characters above simple threshold
    assert(c > MAX_SIMPLE_CHR);

    // Test character against locale-dependent character classes
    // and combine matching class bits to form column index
    if (cm->classbits[CC_PRINT] && pg_wc_isprint(c))
        colnum |= cm->classbits[CC_PRINT];
    if (cm->classbits[CC_ALNUM] && pg_wc_isalnum(c))
        colnum |= cm->classbits[CC_ALNUM];
    if (cm->classbits[CC_ALPHA] && pg_wc_isalpha(c))
        colnum |= cm->classbits[CC_ALPHA];
    if (cm->classbits[CC_WORD] && pg_wc_isword(c))
        colnum |= cm->classbits[CC_WORD];

    // Locale-independent classes are not used (asserted elsewhere)
    assert(cm->classbits[CC_ASCII] == 0);
    assert(cm->classbits[CC_BLANK] == 0);
    assert(cm->classbits[CC_CNTRL] == 0);

    if (cm->classbits[CC_DIGIT] && pg_wc_isdigit(c))
        colnum |= cm->classbits[CC_DIGIT];
    if (cm->classbits[CC_PUNCT] && pg_wc_ispunct(c))
        colnum |= cm->classbits[CC_PUNCT];

    assert(cm->classbits[CC_XDIGIT] == 0);

    if (cm->classbits[CC_SPACE] && pg_wc_isspace(c))
        colnum |= cm->classbits[CC_SPACE];
    if (cm->classbits[CC_LOWER] && pg_wc_islower(c))
        colnum |= cm->classbits[CC_LOWER];
    if (cm->classbits[CC_UPPER] && pg_wc_isupper(c))
        colnum |= cm->classbits[CC_UPPER];
    if (cm->classbits[CC_GRAPH] && pg_wc_isgraph(c))
        colnum |= cm->classbits[CC_GRAPH];

    return colnum;
}
```