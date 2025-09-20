# PostgreSQL contrib directory overview

This document summarizes what each component under `contrib/` contains and what it is used for. The table uses nested rows so you can quickly see each module's key subfolders and files.

[README for contrib directory](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/contrib/README)

| Component | Subpath | Purpose | Key files and subfolders |
|---|---|---|---|
| amcheck | contrib/amcheck | Structural and logical integrity checks for heap and btree indexes | verify_heapam.c, verify_nbtree.c, amcheck.control |
|  | ↳ sql/ | SQL install/upgrade scripts | amcheck--*.sql |
|  | ↳ expected/ | Regression expected outputs | test baselines |
|  | ↳ t/ | TAP tests | Perl tests |
| auth_delay | contrib/auth_delay | Add a fixed delay to failed authentication attempts | auth_delay.c |
| auto_explain | contrib/auto_explain | Log execution plans of slow statements automatically | auto_explain.c |
|  | ↳ sql/, expected/, t/ | Extension SQL, regression, TAP tests | install/upgrade scripts and tests |
| basebackup_to_shell | contrib/basebackup_to_shell | Example base backup sink that writes to shell commands | basebackup_to_shell.c |
|  | ↳ t/ | TAP tests | end-to-end tests |
| basic_archive | contrib/basic_archive | Minimal WAL archiver sample extension | basic_archive.c, basic_archive.conf |
|  | ↳ sql/, expected/ | SQL and regression tests | install script and baselines |
| bloom | contrib/bloom | Bloom filter index access method | blinsert.c, blscan.c, blutils.c, blvacuum.c, bloom.h, bloom.control |
|  | ↳ sql/, expected/, t/ | SQL, regression, TAP tests | install scripts and tests |
| bool_plperl | contrib/bool_plperl | Helper functions to convert booleans for PL/Perl (trusted/untrusted) | bool_plperl.c, *.control |
|  | ↳ sql/, expected/ | SQL and regression tests | install scripts and outputs |
| btree_gin | contrib/btree_gin | B-tree emulation using GIN indexes | btree_gin.c, btree_gin.control |
|  | ↳ sql/, expected/ | SQL and regression tests | install scripts and outputs |
| btree_gist | contrib/btree_gist | B-tree equivalent operator classes for GiST | multiple btree_*.c, btree_gist.c, btree_gist.control |
|  | ↳ data/ | Test datasets | fixture data |
|  | ↳ sql/, expected/ | SQL and regression tests | install scripts and outputs |
| citext | contrib/citext | Case-insensitive character string type | citext.c, citext.control |
|  | ↳ sql/, expected/ | SQL and regression tests | versioned scripts, outputs |
| cube | contrib/cube | n-dimensional cube data type | cube.c, cube.control, cubeparse.y, cubescan.l |
|  | ↳ data/ | Test data | fixture data |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| dblink | contrib/dblink | Connect to other PostgreSQL databases from SQL | dblink.c, dblink.control, pg_service.conf |
|  | ↳ sql/, expected/ | SQL and regression tests | versioned scripts, outputs |
| dict_int | contrib/dict_int | Text search dictionary for integers and ranges | dict_int.c, dict_int.control |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| dict_xsyn | contrib/dict_xsyn | Text search dictionary with synonyms | dict_xsyn.c, dict_xsyn.control, xsyn_sample.rules |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| earthdistance | contrib/earthdistance | Great-circle distance calculations on a sphere (Earth) | earthdistance.c, earthdistance.control |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| file_fdw | contrib/file_fdw | Foreign Data Wrapper for flat files (CSV/TSV) | file_fdw.c, file_fdw.control |
|  | ↳ data/ | Example files for tests | test data |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| fuzzystrmatch | contrib/fuzzystrmatch | String similarity and phonetic matching (Soundex, Metaphone, DM, etc.) | fuzzystrmatch.c, daitch_mokotoff.c, dmetaphone.c, *.control |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| hstore | contrib/hstore | Key-value store within a single PostgreSQL value | hstore.c files (io/op/gin/gist), hstore.control |
|  | ↳ data/ | Test data | fixtures |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| hstore_plperl | contrib/hstore_plperl | Transformations between hstore and Perl hash | hstore_plperl.c, *.control |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| hstore_plpython | contrib/hstore_plpython | Transformations between hstore and Python dict | hstore_plpython.c, *.control |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| intagg | contrib/intagg | Integer aggregation helper (legacy, array-like behavior) | intagg.control, intagg--*.sql |
| intarray | contrib/intarray | Functions, operators, and index support for int[] | many _int*.c, intarray.control |
|  | ↳ bench/ | Benchmarks | scripts/tests for perf |
|  | ↳ data/ | Test data | fixtures |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| isn | contrib/isn | Data types for ISN standards (ISBN, ISSN, EAN, etc.) | isn.c, isn.control, headers for EAN/ISBN/ISSN |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| jsonb_plperl | contrib/jsonb_plperl | Transformations between jsonb and Perl data structures | jsonb_plperl.c, *.control |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| jsonb_plpython | contrib/jsonb_plpython | Transformations between jsonb and Python data structures | jsonb_plpython.c, *.control |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| lo | contrib/lo | Convenience functions for large objects | lo.c, lo.control, lo_test.sql |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| ltree | contrib/ltree | Hierarchical label tree data type and indexes | ltree.c family, ltree.control, crc32.* |
|  | ↳ data/ | Test data | fixtures |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| ltree_plpython | contrib/ltree_plpython | Transformations between ltree and Python | ltree_plpython.c, *.control |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| oid2name | contrib/oid2name | Utility to map OIDs to object names | oid2name.c |
|  | ↳ t/ | TAP tests | command-line tests |
| pageinspect | contrib/pageinspect | Read and decode on-disk page formats (heap, btree, gin, gist, hash, brin) | rawpage.c, brinfuncs.c, btreefuncs.c, ginfuncs.c, gistfuncs.c, hashfuncs.c, heapfuncs.c, pageinspect.control |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| passwordcheck | contrib/passwordcheck | Simple password policy hook | passwordcheck.c |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| pg_buffercache | contrib/pg_buffercache | View contents of the shared buffer cache | pg_buffercache_pages.c, *.control |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| pg_freespacemap | contrib/pg_freespacemap | Examine the Free Space Map | pg_freespacemap.c, pg_freespacemap.conf, *.control |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| pg_prewarm | contrib/pg_prewarm | Load relation blocks into the buffer cache (prewarm) | pg_prewarm.c, autoprewarm.c, *.control |
|  | ↳ sql/, expected/, t/ | SQL, regression, TAP tests | scripts and outputs |
| pg_stat_statements | contrib/pg_stat_statements | Track execution statistics of all SQL statements | pg_stat_statements.c, *.control, pg_stat_statements.conf |
|  | ↳ sql/, expected/, t/ | SQL, regression, TAP tests | scripts and outputs |
| pg_surgery | contrib/pg_surgery | Low-level tools to repair/modify corrupted tables | heap_surgery.c, pg_surgery--*.sql, *.control |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| pg_trgm | contrib/pg_trgm | Text similarity using trigrams with GIN/GiST support | trgm_op.c, trgm_gin.c, trgm_gist.c, trgm_regexp.c, trgm.h, *.control |
|  | ↳ data/ | Test data | fixtures |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| pg_visibility | contrib/pg_visibility | Visibility map and page-level visibility inspection | pg_visibility.c, *.control |
|  | ↳ sql/, expected/, t/ | SQL, regression, TAP tests | scripts and outputs |
| pg_walinspect | contrib/pg_walinspect | Read/analyze WAL records from SQL | pg_walinspect.c, walinspect.conf, *.control |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| pgcrypto | contrib/pgcrypto | Cryptographic functions (hashing, encryption/decryption, PGP) | pgcrypto.c, pgp-*.c, px-*.c, openssl.c, *.control |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| pgrowlocks | contrib/pgrowlocks | Show row-level locks for a table | pgrowlocks.c, *.control |
|  | ↳ specs/ | Test specs | specification files |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| pgstattuple | contrib/pgstattuple | Tuple-level statistics and approximate relation stats | pgstattuple.c, pgstatindex.c, pgstatapprox.c, *.control |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| postgres_fdw | contrib/postgres_fdw | Foreign Data Wrapper for remote PostgreSQL servers | postgres_fdw.c, connection.c, option.c, deparse.c, shippable.c, *.control, postgres_fdw.h |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| seg | contrib/seg | 1-D numeric range (segment) data type and ops | seg.c, segparse.y, segscan.l, segdata.h, seg-validate.pl, sort-segments.pl, *.control |
|  | ↳ data/ | Test data | fixtures |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| sepgsql | contrib/sepgsql | SELinux integration for row/column/table access control | *.c sources, sepgsql.h, sepgsql.sql.in |
|  | ↳ sql/ | SQL install scripts | generated from .in |
|  | ↳ test_sepgsql, launcher | Test suite and helper | SELinux test policy and runner |
|  | ↳ expected/ | Regression outputs | test baselines |
| spi | contrib/spi | Sample triggers/functions using Server Programming Interface | autoinc.c, insert_username.c, moddatetime.c, refint.c and *.sql/control |
| sslinfo | contrib/sslinfo | Expose SSL connection information | sslinfo.c, *.control |
| start-scripts | contrib/start-scripts | Platform service helper scripts for starting PostgreSQL | linux/, freebsd/, [macos/](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/contrib/start-scripts/macos/README) |
| tablefunc | contrib/tablefunc | Table functions like crosstab, connectby, etc. | tablefunc.c, tablefunc.h, *.control |
|  | ↳ sql/, expected/, data/ | SQL, regression tests, data | scripts, outputs, fixtures |
| tcn | contrib/tcn | Triggered change notifications via NOTIFY | tcn.c, *.control |
|  | ↳ specs/ | Test specs | specification files |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| test_decoding | contrib/test_decoding | Example logical decoding output plugin | test_decoding.c, logical.conf |
|  | ↳ specs/ | Test specs | specification files |
|  | ↳ sql/, expected/, t/ | SQL, regression, TAP tests | scripts and outputs |
| tsm_system_rows | contrib/tsm_system_rows | TABLESAMPLE method that returns a fixed number of rows | tsm_system_rows.c, *.control |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| tsm_system_time | contrib/tsm_system_time | TABLESAMPLE method sampling by time | tsm_system_time.c, *.control |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| unaccent | contrib/unaccent | Remove accents (diacritics) from text | unaccent.c, unaccent.rules, generate_unaccent_rules.py, *.control |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| uuid-ossp | contrib/uuid-ossp | UUID generation functions using OSSP library | uuid-ossp.c, *.control |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |
| vacuumlo | contrib/vacuumlo | Remove orphaned large objects (admin tool) | vacuumlo.c |
|  | ↳ t/ | TAP tests | command-line tests |
| xml2 | contrib/xml2 | Additional XML/XPath/XSLT functions via libxml2 | xpath.c, xslt_proc.c, *.control |
|  | ↳ sql/, expected/ | SQL and regression tests | scripts and outputs |

Notes
- Most extensions follow a common layout: C sources, a .control file, versioned SQL scripts under sql/, regression tests under sql/ and expected/, and sometimes TAP tests under t/.
- Some modules include additional folders like data/ (fixtures), specs/ (test specifications), or platform helpers.
- Build files (Makefile, meson.build) are present in each module for both Make and Meson builds.
