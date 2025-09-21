Part VIII. Appendixes  
---  
[Prev](backup-manifest-wal-ranges.md "69.3. Backup Manifest WAL Range Object") | [Up](index.md "PostgreSQL 17.5 Documentation")| PostgreSQL 17.5 Documentation| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](errcodes-appendix.md "Appendix A. PostgreSQL Error Codes")  
  
* * *

# Part VIII. Appendixes

**Table of Contents**

[A. PostgreSQL Error Codes](errcodes-appendix.md)
[B. Date/Time Support](datetime-appendix.md)
    

[B.1. Date/Time Input Interpretation](datetime-input-rules.md)
[B.2. Handling of Invalid or Ambiguous Timestamps](datetime-invalid-input.md)
[B.3. Date/Time Key Words](datetime-keywords.md)
[B.4. Date/Time Configuration Files](datetime-config-files.md)
[B.5. POSIX Time Zone Specifications](datetime-posix-timezone-specs.md)
[B.6. History of Units](datetime-units-history.md)
[B.7. Julian Dates](datetime-julian-dates.md)
[C. SQL Key Words](sql-keywords-appendix.md)
[D. SQL Conformance](features.md)
    

[D.1. Supported Features](features-sql-standard.md)
[D.2. Unsupported Features](unsupported-features-sql-standard.md)
[D.3. XML Limits and Conformance to SQL/XML](xml-limits-conformance.md)
[E. Release Notes](release.md)
    

[E.1. Release 17.5](release-17-5.md)
[E.2. Release 17.4](release-17-4.md)
[E.3. Release 17.3](release-17-3.md)
[E.4. Release 17.2](release-17-2.md)
[E.5. Release 17.1](release-17-1.md)
[E.6. Release 17](release-17.md)
[E.7. Prior Releases](release-prior.md)
[F. Additional Supplied Modules and Extensions](contrib.md)
    

[F.1. amcheck — tools to verify table and index consistency](amcheck.md)
[F.2. auth_delay — pause on authentication failure](auth-delay.md)
[F.3. auto_explain — log execution plans of slow queries](auto-explain.md)
[F.4. basebackup_to_shell — example "shell" pg_basebackup module](basebackup-to-shell.md)
[F.5. basic_archive — an example WAL archive module](basic-archive.md)
[F.6. bloom — bloom filter index access method](bloom.md)
[F.7. btree_gin — GIN operator classes with B-tree behavior](btree-gin.md)
[F.8. btree_gist — GiST operator classes with B-tree behavior](btree-gist.md)
[F.9. citext — a case-insensitive character string type](citext.md)
[F.10. cube — a multi-dimensional cube data type](cube.md)
[F.11. dblink — connect to other PostgreSQL databases](dblink.md)
[F.12. dict_int — example full-text search dictionary for integers](dict-int.md)
[F.13. dict_xsyn — example synonym full-text search dictionary](dict-xsyn.md)
[F.14. earthdistance — calculate great-circle distances](earthdistance.md)
[F.15. file_fdw — access data files in the server's file system](file-fdw.md)
[F.16. fuzzystrmatch — determine string similarities and distance](fuzzystrmatch.md)
[F.17. hstore — hstore key/value datatype](hstore.md)
[F.18. intagg — integer aggregator and enumerator](intagg.md)
[F.19. intarray — manipulate arrays of integers](intarray.md)
[F.20. isn — data types for international standard numbers (ISBN, EAN, UPC, etc.)](isn.md)
[F.21. lo — manage large objects](lo.md)
[F.22. ltree — hierarchical tree-like data type](ltree.md)
[F.23. pageinspect — low-level inspection of database pages](pageinspect.md)
[F.24. passwordcheck — verify password strength](passwordcheck.md)
[F.25. pg_buffercache — inspect PostgreSQL buffer cache state](pgbuffercache.md)
[F.26. pgcrypto — cryptographic functions](pgcrypto.md)
[F.27. pg_freespacemap — examine the free space map](pgfreespacemap.md)
[F.28. pg_prewarm — preload relation data into buffer caches](pgprewarm.md)
[F.29. pgrowlocks — show a table's row locking information](pgrowlocks.md)
[F.30. pg_stat_statements — track statistics of SQL planning and execution](pgstatstatements.md)
[F.31. pgstattuple — obtain tuple-level statistics](pgstattuple.md)
[F.32. pg_surgery — perform low-level surgery on relation data](pgsurgery.md)
[F.33. pg_trgm — support for similarity of text using trigram matching](pgtrgm.md)
[F.34. pg_visibility — visibility map information and utilities](pgvisibility.md)
[F.35. pg_walinspect — low-level WAL inspection](pgwalinspect.md)
[F.36. postgres_fdw — access data stored in external PostgreSQL servers](postgres-fdw.md)
[F.37. seg — a datatype for line segments or floating point intervals](seg.md)
[F.38. sepgsql — SELinux-, label-based mandatory access control (MAC) security module](sepgsql.md)
[F.39. spi — Server Programming Interface features/examples](contrib-spi.md)
[F.40. sslinfo — obtain client SSL information](sslinfo.md)
[F.41. tablefunc — functions that return tables (`crosstab` and others)](tablefunc.md)
[F.42. tcn — a trigger function to notify listeners of changes to table content](tcn.md)
[F.43. test_decoding — SQL-based test/example module for WAL logical decoding](test-decoding.md)
[F.44. tsm_system_rows — the `SYSTEM_ROWS` sampling method for `TABLESAMPLE`](tsm-system-rows.md)
[F.45. tsm_system_time — the `SYSTEM_TIME` sampling method for `TABLESAMPLE`](tsm-system-time.md)
[F.46. unaccent — a text search dictionary which removes diacritics](unaccent.md)
[F.47. uuid-ossp — a UUID generator](uuid-ossp.md)
[F.48. xml2 — XPath querying and XSLT functionality](xml2.md)
[G. Additional Supplied Programs](contrib-prog.md)
    

[G.1. Client Applications](contrib-prog-client.md)
[G.2. Server Applications](contrib-prog-server.md)
[H. External Projects](external-projects.md)
    

[H.1. Client Interfaces](external-interfaces.md)
[H.2. Administration Tools](external-admin-tools.md)
[H.3. Procedural Languages](external-pl.md)
[H.4. Extensions](external-extensions.md)
[I. The Source Code Repository](sourcerepo.md)
    

[I.1. Getting the Source via Git](git.md)
[J. Documentation](docguide.md)
    

[J.1. DocBook](docguide-docbook.md)
[J.2. Tool Sets](docguide-toolsets.md)
[J.3. Building the Documentation with Make](docguide-build.md)
[J.4. Building the Documentation with Meson](docguide-build-meson.md)
[J.5. Documentation Authoring](docguide-authoring.md)
[J.6. Style Guide](docguide-style.md)
[K. PostgreSQL Limits](limits.md)
[L. Acronyms](acronyms.md)
[M. Glossary](glossary.md)
[N. Color Support](color.md)
    

[N.1. When Color is Used](color-when.md)
[N.2. Configuring the Colors](color-which.md)
[O. Obsolete or Renamed Features](appendix-obsolete.md)
    

[O.1. `recovery.conf` file merged into `postgresql.conf`](recovery-config.md)
[O.2. Default Roles Renamed to Predefined Roles](default-roles.md)
[O.3. `pg_xlogdump` renamed to `pg_waldump`](pgxlogdump.md)
[O.4. `pg_resetxlog` renamed to `pg_resetwal`](app-pgresetxlog.md)
[O.5. `pg_receivexlog` renamed to `pg_receivewal`](app-pgreceivexlog.md)

* * *

[Prev](backup-manifest-wal-ranges.md "69.3. Backup Manifest WAL Range Object") | [Up](index.md "PostgreSQL 17.5 Documentation")|  [Next](errcodes-appendix.md "Appendix A. PostgreSQL Error Codes")  
---|---|---  
69.3. Backup Manifest WAL Range Object | [Home](index.md "PostgreSQL 17.5 Documentation")|  Appendix A. PostgreSQL Error Codes
