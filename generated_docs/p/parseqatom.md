# parseqatom

## Location
[src/backend/regex/regcomp.c:838-847](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/regex/regcomp.c#L838-L847)

## Overview
Parses one quantified atom or constraint of a regular expression, handling both simple atoms (like characters or character classes) and complex structures (like capturing groups and backreferences) along with their optional quantifiers.

## Definition
```c
static struct subre *
parseqatom(struct vars *v,
           int stopper,          /* EOS or ') __pycache__/ config/ contrib/ data/ default/ doc/ output/ scripts/ src/ sys/ test/ venv/ views/