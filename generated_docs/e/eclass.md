# eclass

## Location
[src/backend/regex/regc_locale.c:500-535](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regc_locale.c#L500-L535)

## Overview
The eclass function creates a character vector representing an equivalence class for collating elements in PostgreSQL's regular expression engine, with optional case variant inclusion.

## Definition

```c
struct vars *v,			/* context */
	   chr c,					/* Collating element representing the
								 * equivalence class. */
	   int cases)				/* all cases? */
{
	struct cvec *cv;

	/* crude fake equivalence class for testing */
	if ((v->cflags & REG_FAKE) && c == 'x')
	{
		cv = getcvec(v, 4, 0);
		addchr(cv, CHR('x'));
		addchr(cv, CHR('y'));
		if (cases)
		{
			addchr(cv, CHR('X'));
			addchr(cv, CHR('Y'));
		}
		return cv;
	}

	/* otherwise, none */
	if (cases)
		return allcases(v, c);
	cv = getcvec(v, 1, 0);
	assert(cv != NULL);
	addchr(cv, c);
	return cv;
}

/*
 * lookupcclass - lookup a character class identified by name
 *
 * On failure, sets an error code in *v;
```
## Detailed Description
The eclass function implements equivalence class processing for PostgreSQL's regular expression bracket expressions. Equivalence classes group characters that should be treated as equivalent for collation purposes, such as characters with different accents that sort to the same position.

The function currently provides a minimal implementation with the following behavior:

1. **Test mode**: When the REG_FAKE flag is set and the character is 'x', it creates a fake equivalence class containing 'x' and 'y' (and their uppercase variants if case-independent mode is requested). This is used for testing purposes.

2. **Case-independent mode**: When cases is true, it delegates to the allcases() function to generate all case variants of the given character.

3. **Standard mode**: For normal operation, it simply creates a single-character cvec containing only the input character, effectively treating each character as its own equivalence class.

This minimal implementation reflects that PostgreSQL does not currently implement full collating element equivalence classes, instead treating most characters as equivalent only to themselves.

## Parameters / Member Variables
- : Context structure containing regex compilation state and configuration flags
- : The collating element (character) that represents the equivalence class to be generated  
- : Flag indicating whether to include case variants (non-zero for case-independent matching)

## Dependencies
- Functions called/Symbols referenced:
  - getcvec (character vector allocation)
  - addchr (add individual character to cvec)
  - allcases (generate case variants)
  - CHR (character conversion macro)
  - REG_FAKE (test mode flag)
- Called from (representative examples):
  - [brackpart](../b/brackpart.md) (in regcomp.c:1808 for bracket expression equivalence classes)

## Notes and Other Information
- Currently provides minimal equivalence class support - most characters are equivalent only to themselves
- The REG_FAKE test mode demonstrates how a more complete equivalence class system might work
- Part of PostgreSQL's broader character classification system for regular expressions
- Returns a newly allocated cvec that the caller is responsible for managing
- In true equivalence class systems, characters like 'é', 'è', and 'e' might all belong to the same equivalence class