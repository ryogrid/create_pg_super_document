F.25. pg_buffercache — inspect PostgreSQL buffer cache state  
---  
[Prev](passwordcheck.md "F.24. passwordcheck — verify password strength") | [Up](contrib.md "Appendix F. Additional Supplied Modules and Extensions")| Appendix F. Additional Supplied Modules and Extensions| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](pgcrypto.md "F.26. pgcrypto — cryptographic functions")  
  
* * *

## F.25. pg_buffercache — inspect PostgreSQL buffer cache state #

[F.25.1. The `pg_buffercache` View](pgbuffercache.md#PGBUFFERCACHE-PG-BUFFERCACHE)
[F.25.2. The `pg_buffercache_summary()` Function](pgbuffercache.md#PGBUFFERCACHE-SUMMARY)
[F.25.3. The `pg_buffercache_usage_counts()` Function](pgbuffercache.md#PGBUFFERCACHE-USAGE-COUNTS)
[F.25.4. The `pg_buffercache_evict()` Function](pgbuffercache.md#PGBUFFERCACHE-PG-BUFFERCACHE-EVICT)
[F.25.5. Sample Output](pgbuffercache.md#PGBUFFERCACHE-SAMPLE-OUTPUT)
[F.25.6. Authors](pgbuffercache.md#PGBUFFERCACHE-AUTHORS)

The `pg_buffercache` module provides a means for examining what's happening in the shared buffer cache in real time. It also offers a low-level way to evict data from it, for testing purposes. 

This module provides the `pg_buffercache_pages()` function (wrapped in the `pg_buffercache` view), the `pg_buffercache_summary()` function, the `pg_buffercache_usage_counts()` function and the `pg_buffercache_evict()` function. 

The `pg_buffercache_pages()` function returns a set of records, each row describing the state of one shared buffer entry. The `pg_buffercache` view wraps the function for convenient use. 

The `pg_buffercache_summary()` function returns a single row summarizing the state of the shared buffer cache. 

The `pg_buffercache_usage_counts()` function returns a set of records, each row describing the number of buffers with a given usage count. 

By default, use of the above functions is restricted to superusers and roles with privileges of the `pg_monitor` role. Access may be granted to others using `GRANT`. 

The `pg_buffercache_evict()` function allows a block to be evicted from the buffer pool given a buffer identifier. Use of this function is restricted to superusers only. 

### F.25.1. The `pg_buffercache` View #

The definitions of the columns exposed by the view are shown in [Table F.14](pgbuffercache.md#PGBUFFERCACHE-COLUMNS "Table F.14. pg_buffercache Columns"). 

**Table F.14.`pg_buffercache` Columns**

Column Type  Description   
---  
`bufferid` `integer` ID, in the range 1..`shared_buffers`  
`relfilenode` `oid` (references [`pg_class`](catalog-pg-class.md "51.11. pg_class").`relfilenode`)  Filenode number of the relation   
`reltablespace` `oid` (references [`pg_tablespace`](catalog-pg-tablespace.md "51.56. pg_tablespace").`oid`)  Tablespace OID of the relation   
`reldatabase` `oid` (references [`pg_database`](catalog-pg-database.md "51.15. pg_database").`oid`)  Database OID of the relation   
`relforknumber` `smallint` Fork number within the relation; see `common/relpath.h`  
`relblocknumber` `bigint` Page number within the relation   
`isdirty` `boolean` Is the page dirty?   
`usagecount` `smallint` Clock-sweep access count   
`pinning_backends` `integer` Number of backends pinning this buffer   
  
  


There is one row for each buffer in the shared cache. Unused buffers are shown with all fields null except `bufferid`. Shared system catalogs are shown as belonging to database zero. 

Because the cache is shared by all the databases, there will normally be pages from relations not belonging to the current database. This means that there may not be matching join rows in `pg_class` for some rows, or that there could even be incorrect joins. If you are trying to join against `pg_class`, it's a good idea to restrict the join to rows having `reldatabase` equal to the current database's OID or zero. 

Since buffer manager locks are not taken to copy the buffer state data that the view will display, accessing `pg_buffercache` view has less impact on normal buffer activity but it doesn't provide a consistent set of results across all buffers. However, we ensure that the information of each buffer is self-consistent. 

### F.25.2. The `pg_buffercache_summary()` Function #

The definitions of the columns exposed by the function are shown in [Table F.15](pgbuffercache.md#PGBUFFERCACHE-SUMMARY-COLUMNS "Table F.15. pg_buffercache_summary\(\) Output Columns"). 

**Table F.15.`pg_buffercache_summary()` Output Columns**

Column Type  Description   
---  
`buffers_used` `int4` Number of used shared buffers   
`buffers_unused` `int4` Number of unused shared buffers   
`buffers_dirty` `int4` Number of dirty shared buffers   
`buffers_pinned` `int4` Number of pinned shared buffers   
`usagecount_avg` `float8` Average usage count of used shared buffers   
  
  


The `pg_buffercache_summary()` function returns a single row summarizing the state of all shared buffers. Similar and more detailed information is provided by the `pg_buffercache` view, but `pg_buffercache_summary()` is significantly cheaper. 

Like the `pg_buffercache` view, `pg_buffercache_summary()` does not acquire buffer manager locks. Therefore concurrent activity can lead to minor inaccuracies in the result. 

### F.25.3. The `pg_buffercache_usage_counts()` Function #

The definitions of the columns exposed by the function are shown in [Table F.16](pgbuffercache.md#PGBUFFERCACHE_USAGE_COUNTS-COLUMNS "Table F.16. pg_buffercache_usage_counts\(\) Output Columns"). 

**Table F.16.`pg_buffercache_usage_counts()` Output Columns**

Column Type  Description   
---  
`usage_count` `int4` A possible buffer usage count   
`buffers` `int4` Number of buffers with the usage count   
`dirty` `int4` Number of dirty buffers with the usage count   
`pinned` `int4` Number of pinned buffers with the usage count   
  
  


The `pg_buffercache_usage_counts()` function returns a set of rows summarizing the states of all shared buffers, aggregated over the possible usage count values. Similar and more detailed information is provided by the `pg_buffercache` view, but `pg_buffercache_usage_counts()` is significantly cheaper. 

Like the `pg_buffercache` view, `pg_buffercache_usage_counts()` does not acquire buffer manager locks. Therefore concurrent activity can lead to minor inaccuracies in the result. 

### F.25.4. The `pg_buffercache_evict()` Function #

The `pg_buffercache_evict()` function takes a buffer identifier, as shown in the `bufferid` column of the `pg_buffercache` view. It returns true on success, and false if the buffer wasn't valid, if it couldn't be evicted because it was pinned, or if it became dirty again after an attempt to write it out. The result is immediately out of date upon return, as the buffer might become valid again at any time due to concurrent activity. The function is intended for developer testing only. 

### F.25.5. Sample Output #
    
    
    regression=# SELECT n.nspname, c.relname, count(*) AS buffers
                 FROM pg_buffercache b JOIN pg_class c
                 ON b.relfilenode = pg_relation_filenode(c.oid) AND
                    b.reldatabase IN (0, (SELECT oid FROM pg_database
                                          WHERE datname = current_database()))
                 JOIN pg_namespace n ON n.oid = c.relnamespace
                 GROUP BY n.nspname, c.relname
                 ORDER BY 3 DESC
                 LIMIT 10;
    
      nspname   |        relname         | buffers
    ------------+------------------------+---------
     public     | delete_test_table      |     593
     public     | delete_test_table_pkey |     494
     pg_catalog | pg_attribute           |     472
     public     | quad_poly_tbl          |     353
     public     | tenk2                  |     349
     public     | tenk1                  |     349
     public     | gin_test_idx           |     306
     pg_catalog | pg_largeobject         |     206
     public     | gin_test_tbl           |     188
     public     | spgist_text_tbl        |     182
    (10 rows)
    
    
    regression=# SELECT * FROM pg_buffercache_summary();
     buffers_used | buffers_unused | buffers_dirty | buffers_pinned | usagecount_avg
    --------------+----------------+---------------+----------------+----------------
              248 |        2096904 |            39 |              0 |       3.141129
    (1 row)
    
    
    regression=# SELECT * FROM pg_buffercache_usage_counts();
     usage_count | buffers | dirty | pinned
    -------------+---------+-------+--------
               0 |   14650 |     0 |      0
               1 |    1436 |   671 |      0
               2 |     102 |    88 |      0
               3 |      23 |    21 |      0
               4 |       9 |     7 |      0
               5 |     164 |   106 |      0
    (6 rows)
    

### F.25.6. Authors #

Mark Kirkwood `<[markir@paradise.net.nz](mailto:markir@paradise.net.nz)>`

Design suggestions: Neil Conway `<[neilc@samurai.com](mailto:neilc@samurai.com)>`

Debugging advice: Tom Lane `<[tgl@sss.pgh.pa.us](mailto:tgl@sss.pgh.pa.us)>`

* * *

[Prev](passwordcheck.md "F.24. passwordcheck — verify password strength") | [Up](contrib.md "Appendix F. Additional Supplied Modules and Extensions")|  [Next](pgcrypto.md "F.26. pgcrypto — cryptographic functions")  
---|---|---  
F.24. passwordcheck — verify password strength | [Home](index.md "PostgreSQL 17.5 Documentation")|  F.26. pgcrypto — cryptographic functions
