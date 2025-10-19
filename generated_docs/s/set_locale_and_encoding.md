# set_locale_and_encoding

## Location
[src/bin/pg_upgrade/pg_upgrade.c:405-483](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_upgrade/pg_upgrade.c#L405-L483)

## Overview
Copies locale and encoding information from the old cluster's template0 database to the new cluster's template0, ensuring consistent locale settings during pg_upgrade.

## Definition

```c
static void
set_locale_and_encoding(void)
```
## Detailed Description
The set_locale_and_encoding function transfers critical locale and encoding configuration from the old cluster to the new cluster by updating the template0 database in the new cluster. This ensures that all databases created from template0 will inherit the correct locale settings. The function:

- Extracts locale information from the old cluster's template0 database
- Connects to the new cluster's template1 database to perform updates
- Escapes locale strings safely using PQescapeLiteral 
- Updates encoding, datlocprovider, datcollate, datctype, and datlocale fields
- Handles version-specific differences in locale field names (datlocale vs daticulocale)
- Supports PostgreSQL versions 15.0+ with locale provider functionality and 17.0+ with enhanced locale handling

The function is version-aware and adapts the SQL update statement based on the target PostgreSQL version.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [prep_status](../p/prep_status.md) (status reporting)
  - [connectToServer](../c/connectToServer.md) (database connection)
  - [PQescapeLiteral](../P/PQescapeLiteral.md) (SQL string escaping)
  - [executeQueryOrDie](../e/executeQueryOrDie.md) (SQL execution)
  - GET_MAJOR_VERSION (version checking)
  - [PQfreemem](../P/PQfreemem.md) (memory cleanup)
  - [PQfinish](../P/PQfinish.md) (connection cleanup)
  - [check_ok](../c/check_ok.md) (status verification)
  - DbLocaleInfo (locale information structure)
- Called from:
  - [main](../m/main.md) (from pg_upgrade.c:151)

## Notes and Other Information
- Critical for maintaining locale consistency across PostgreSQL versions
- Handles version differences between PostgreSQL 15+ (daticulocale) and 17+ (datlocale)
- Does not copy datcollversion as it's never set for template0
- Uses proper SQL escaping to prevent injection issues with locale strings
- Essential for ensuring that new databases created after upgrade have correct locale settings
- Part of the schema transfer phase of pg_upgrade process

## Simplified Source

```c
static void set_locale_and_encoding(void) {
    PGconn *conn_new_template1;
    char *datcollate_literal;
    char *datctype_literal;
    char *datlocale_literal = NULL;
    DbLocaleInfo *locale = old_cluster.template0;

    prep_status("Setting locale and encoding for new cluster");

    // Connect to new cluster template1 for updates
    conn_new_template1 = connectToServer(&new_cluster, "template1");

    // Escape locale strings for safe SQL embedding
    datcollate_literal = PQescapeLiteral(conn_new_template1,
                                        locale->db_collate,
                                        strlen(locale->db_collate));
    datctype_literal = PQescapeLiteral(conn_new_template1,
                                      locale->db_ctype,
                                      strlen(locale->db_ctype));

    if (locale->db_locale)
        datlocale_literal = PQescapeLiteral(conn_new_template1,
                                           locale->db_locale,
                                           strlen(locale->db_locale));
    else
        datlocale_literal = "NULL";

    // Update template0 with locale settings based on PostgreSQL version
    if (GET_MAJOR_VERSION(new_cluster.major_version) >= 1700) {
        // PostgreSQL 17+: uses datlocale field
        PQclear(executeQueryOrDie(conn_new_template1,
                "UPDATE pg_catalog.pg_database "
                "SET encoding = %d, datlocprovider = '%c', "
                "    datcollate = %s, datctype = %s, datlocale = %s "
                "WHERE datname = 'template0'",
                locale->db_encoding, locale->db_collprovider,
                datcollate_literal, datctype_literal, datlocale_literal));
    }
    else if (GET_MAJOR_VERSION(new_cluster.major_version) >= 1500) {
        // PostgreSQL 15-16: uses daticulocale field
        PQclear(executeQueryOrDie(conn_new_template1,
                "UPDATE pg_catalog.pg_database "
                "SET encoding = %d, datlocprovider = '%c', "
                "    datcollate = %s, datctype = %s, daticulocale = %s "
                "WHERE datname = 'template0'",
                locale->db_encoding, locale->db_collprovider,
                datcollate_literal, datctype_literal, datlocale_literal));
    }
    else {
        // PostgreSQL < 15: basic locale fields only
        PQclear(executeQueryOrDie(conn_new_template1,
                "UPDATE pg_catalog.pg_database "
                "SET encoding = %d, datcollate = %s, datctype = %s "
                "WHERE datname = 'template0'",
                locale->db_encoding, datcollate_literal, datctype_literal));
    }

    // Clean up memory and connections
    PQfreemem(datcollate_literal);
    PQfreemem(datctype_literal);
    if (locale->db_locale)
        PQfreemem(datlocale_literal);
    PQfinish(conn_new_template1);

    check_ok();
}
```