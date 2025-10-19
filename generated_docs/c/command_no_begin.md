# command_no_begin

## Location
[src/bin/psql/common.c:1897-2111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L1897-L2111)

## Overview
command_no_begin determines whether a SQL command should NOT be preceded by an automatic BEGIN statement, identifying transaction control commands and statements prohibited within transaction blocks.

## Definition
static bool command_no_begin(const char *query)

## Detailed Description
command_no_begin implements psql's autocommit logic by parsing SQL commands to determine if they should be executed outside of a transaction block. When autocommit is disabled, psql normally wraps user commands in BEGIN/COMMIT, but certain commands must be excluded from this behavior.

**Categories of Commands Detected:**

1. **Transaction Control Commands:**
   - ABORT, BEGIN, START (TRANSACTION)
   - COMMIT, END, ROLLBACK
   - PREPARE TRANSACTION (but not PREPARE statement)

2. **Commands Prohibited in Transaction Blocks:**
   - VACUUM (all variants)
   - CLUSTER (without arguments only)
   - CREATE DATABASE, CREATE TABLESPACE
   - CREATE [UNIQUE] INDEX CONCURRENTLY
   - ALTER SYSTEM
   - DROP DATABASE, DROP TABLESPACE
   - DROP INDEX CONCURRENTLY
   - REINDEX DATABASE, REINDEX SYSTEM, REINDEX TABLESPACE
   - REINDEX [TABLE|INDEX] CONCURRENTLY
   - DISCARD ALL (but not other DISCARD variants)

**Parsing Strategy:**
- Uses skip_white_space() to handle comments and whitespace
- Implements case-insensitive keyword matching with pg_strncasecmp()
- Performs word boundary checking to avoid partial matches
- Handles complex multi-word commands with proper tokenization
- Uses PQmblenBounded() for multibyte character support

The function ensures that commands calling PreventInTransactionBlock() in the PostgreSQL backend are correctly identified to prevent transaction block wrapping.

## Parameters / Member Variables
- `query`: The SQL command string to analyze

## Dependencies
- Functions called/Symbols referenced:
  - [skip_white_space](../s/skip_white_space.md) (for parsing whitespace and comments)
  - [pg_strncasecmp](../p/pg_strncasecmp.md) (for case-insensitive string comparison)
  - [PQmblenBounded](../P/PQmblenBounded.md) (for multibyte character handling)
  - isalpha (for character classification)
- Called from (representative examples):
  - [SendQuery](../S/SendQuery.md) (to determine autocommit behavior)

## Notes and Other Information
- Returns true if the command should NOT be wrapped in BEGIN/COMMIT, false otherwise
- Function is static, only accessible within common.c
- Critical for psql's autocommit=off mode functionality
- Handles PostgreSQL-specific command variants (e.g., CREATE INDEX CONCURRENTLY)
- Performs precise parsing to distinguish similar commands (e.g., PREPARE vs PREPARE TRANSACTION)
- Encoding-aware parsing using multibyte character functions
- Matches exactly the commands that call PreventInTransactionBlock() in PostgreSQL backend
- Some edge cases like "DROP SYSTEM" are intentionally over-matched as they're invalid anyway
- Essential for maintaining transaction semantics in interactive PostgreSQL sessions

## Simplified Source

```c
static bool command_no_begin(const char *query)
{
    int wordlen;

    // Skip whitespace and comments
    query = skip_white_space(query);

    // Get length of first word
    wordlen = 0;
    while (isalpha((unsigned char) query[wordlen]))
        wordlen += PQmblenBounded(&query[wordlen], pset.encoding);

    // Transaction control commands - these manage transactions themselves
    if (wordlen == 5 && pg_strncasecmp(query, "abort", 5) == 0)
        return true;
    if (wordlen == 5 && pg_strncasecmp(query, "begin", 5) == 0)
        return true;
    if (wordlen == 5 && pg_strncasecmp(query, "start", 5) == 0)
        return true;
    if (wordlen == 6 && pg_strncasecmp(query, "commit", 6) == 0)
        return true;
    if (wordlen == 3 && pg_strncasecmp(query, "end", 3) == 0)
        return true;
    if (wordlen == 8 && pg_strncasecmp(query, "rollback", 8) == 0)
        return true;

    // PREPARE TRANSACTION (but not PREPARE statement)
    if (wordlen == 7 && pg_strncasecmp(query, "prepare", 7) == 0) {
        query += wordlen;
        query = skip_white_space(query);

        wordlen = 0;
        while (isalpha((unsigned char) query[wordlen]))
            wordlen += PQmblenBounded(&query[wordlen], pset.encoding);

        if (wordlen == 11 && pg_strncasecmp(query, "transaction", 11) == 0)
            return true;
        return false;
    }

    // Commands not allowed within transactions
    if (wordlen == 6 && pg_strncasecmp(query, "vacuum", 6) == 0)
        return true;

    // CLUSTER without arguments
    if (wordlen == 7 && pg_strncasecmp(query, "cluster", 7) == 0) {
        query += wordlen;
        query = skip_white_space(query);

        if (isalpha((unsigned char) query[0]))
            return false; // has arguments
        return true; // CLUSTER without arguments
    }

    // CREATE DATABASE, CREATE TABLESPACE, CREATE INDEX CONCURRENTLY
    if (wordlen == 6 && pg_strncasecmp(query, "create", 6) == 0) {
        query += wordlen;
        query = skip_white_space(query);

        wordlen = 0;
        while (isalpha((unsigned char) query[wordlen]))
            wordlen += PQmblenBounded(&query[wordlen], pset.encoding);

        if (wordlen == 8 && pg_strncasecmp(query, "database", 8) == 0)
            return true;
        if (wordlen == 10 && pg_strncasecmp(query, "tablespace", 10) == 0)
            return true;

        // Handle CREATE [UNIQUE] INDEX CONCURRENTLY
        if (wordlen == 6 && pg_strncasecmp(query, "unique", 6) == 0) {
            query += wordlen;
            query = skip_white_space(query);

            wordlen = 0;
            while (isalpha((unsigned char) query[wordlen]))
                wordlen += PQmblenBounded(&query[wordlen], pset.encoding);
        }

        if (wordlen == 5 && pg_strncasecmp(query, "index", 5) == 0) {
            query += wordlen;
            query = skip_white_space(query);

            wordlen = 0;
            while (isalpha((unsigned char) query[wordlen]))
                wordlen += PQmblenBounded(&query[wordlen], pset.encoding);

            if (wordlen == 12 && pg_strncasecmp(query, "concurrently", 12) == 0)
                return true;
        }

        return false;
    }

    // ALTER SYSTEM
    if (wordlen == 5 && pg_strncasecmp(query, "alter", 5) == 0) {
        query += wordlen;
        query = skip_white_space(query);

        wordlen = 0;
        while (isalpha((unsigned char) query[wordlen]))
            wordlen += PQmblenBounded(&query[wordlen], pset.encoding);

        if (wordlen == 6 && pg_strncasecmp(query, "system", 6) == 0)
            return true;

        return false;
    }

    // DROP/REINDEX variants
    if ((wordlen == 4 && pg_strncasecmp(query, "drop", 4) == 0) ||
        (wordlen == 7 && pg_strncasecmp(query, "reindex", 7) == 0)) {
        query += wordlen;
        query = skip_white_space(query);

        wordlen = 0;
        while (isalpha((unsigned char) query[wordlen]))
            wordlen += PQmblenBounded(&query[wordlen], pset.encoding);

        if (wordlen == 8 && pg_strncasecmp(query, "database", 8) == 0)
            return true;
        if (wordlen == 6 && pg_strncasecmp(query, "system", 6) == 0)
            return true;
        if (wordlen == 10 && pg_strncasecmp(query, "tablespace", 10) == 0)
            return true;

        // Handle REINDEX [TABLE|INDEX] CONCURRENTLY
        if (wordlen == 5 && (pg_strncasecmp(query, "index", 5) == 0 ||
                            pg_strncasecmp(query, "table", 5) == 0)) {
            query += wordlen;
            query = skip_white_space(query);

            wordlen = 0;
            while (isalpha((unsigned char) query[wordlen]))
                wordlen += PQmblenBounded(&query[wordlen], pset.encoding);

            if (wordlen == 12 && pg_strncasecmp(query, "concurrently", 12) == 0)
                return true;
        }

        // DROP INDEX CONCURRENTLY
        if (wordlen == 5 && pg_strncasecmp(query, "index", 5) == 0) {
            query += wordlen;
            query = skip_white_space(query);

            wordlen = 0;
            while (isalpha((unsigned char) query[wordlen]))
                wordlen += PQmblenBounded(&query[wordlen], pset.encoding);

            if (wordlen == 12 && pg_strncasecmp(query, "concurrently", 12) == 0)
                return true;

            return false;
        }

        return false;
    }

    // DISCARD ALL (but not other DISCARD variants)
    if (wordlen == 7 && pg_strncasecmp(query, "discard", 7) == 0) {
        query += wordlen;
        query = skip_white_space(query);

        wordlen = 0;
        while (isalpha((unsigned char) query[wordlen]))
            wordlen += PQmblenBounded(&query[wordlen], pset.encoding);

        if (wordlen == 3 && pg_strncasecmp(query, "all", 3) == 0)
            return true;
        return false;
    }

    return false;
}
```