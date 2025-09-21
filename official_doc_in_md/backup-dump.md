25.1. SQL Dump  
---  
[Prev](backup.md "Chapter 25. Backup and Restore") | [Up](backup.md "Chapter 25. Backup and Restore")| Chapter 25. Backup and Restore| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](backup-file.md "25.2. File System Level Backup")  
  
* * *

## 25.1. SQL Dump #

[25.1.1. Restoring the Dump](backup-dump.md#BACKUP-DUMP-RESTORE)
[25.1.2. Using pg_dumpall](backup-dump.md#BACKUP-DUMP-ALL)
[25.1.3. Handling Large Databases](backup-dump.md#BACKUP-DUMP-LARGE)

The idea behind this dump method is to generate a file with SQL commands that, when fed back to the server, will recreate the database in the same state as it was at the time of the dump. PostgreSQL provides the utility program [pg_dump](app-pgdump.md "pg_dump") for this purpose. The basic usage of this command is: 
    
    
    pg_dump _dbname_ > _dumpfile_
    

As you see, pg_dump writes its result to the standard output. We will see below how this can be useful. While the above command creates a text file, pg_dump can create files in other formats that allow for parallelism and more fine-grained control of object restoration. 

pg_dump is a regular PostgreSQL client application (albeit a particularly clever one). This means that you can perform this backup procedure from any remote host that has access to the database. But remember that pg_dump does not operate with special permissions. In particular, it must have read access to all tables that you want to back up, so in order to back up the entire database you almost always have to run it as a database superuser. (If you do not have sufficient privileges to back up the entire database, you can still back up portions of the database to which you do have access using options such as `-n _`schema`_` or `-t _`table`_`.) 

To specify which database server pg_dump should contact, use the command line options `-h _`host`_` and `-p _`port`_`. The default host is the local host or whatever your `PGHOST` environment variable specifies. Similarly, the default port is indicated by the `PGPORT` environment variable or, failing that, by the compiled-in default. (Conveniently, the server will normally have the same compiled-in default.) 

Like any other PostgreSQL client application, pg_dump will by default connect with the database user name that is equal to the current operating system user name. To override this, either specify the `-U` option or set the environment variable `PGUSER`. Remember that pg_dump connections are subject to the normal client authentication mechanisms (which are described in [Chapter 20](client-authentication.md "Chapter 20. Client Authentication")). 

An important advantage of pg_dump over the other backup methods described later is that pg_dump's output can generally be re-loaded into newer versions of PostgreSQL, whereas file-level backups and continuous archiving are both extremely server-version-specific. pg_dump is also the only method that will work when transferring a database to a different machine architecture, such as going from a 32-bit to a 64-bit server. 

Dumps created by pg_dump are internally consistent, meaning, the dump represents a snapshot of the database at the time pg_dump began running. pg_dump does not block other operations on the database while it is working. (Exceptions are those operations that need to operate with an exclusive lock, such as most forms of `ALTER TABLE`.) 

### 25.1.1. Restoring the Dump #

Text files created by pg_dump are intended to be read by the psql program using its default settings. The general command form to restore a text dump is 
    
    
    psql -X _dbname_ < _dumpfile_
    

where _`dumpfile`_ is the file output by the pg_dump command. The database _`dbname`_ will not be created by this command, so you must create it yourself from `template0` before executing psql (e.g., with `createdb -T template0 _`dbname`_`). To ensure psql runs with its default settings, use the `-X` (`--no-psqlrc`) option. psql supports options similar to pg_dump for specifying the database server to connect to and the user name to use. See the [psql](app-psql.md "psql") reference page for more information. 

Non-text file dumps should be restored using the [pg_restore](app-pgrestore.md "pg_restore") utility. 

Before restoring an SQL dump, all the users who own objects or were granted permissions on objects in the dumped database must already exist. If they do not, the restore will fail to recreate the objects with the original ownership and/or permissions. (Sometimes this is what you want, but usually it is not.) 

By default, the psql script will continue to execute after an SQL error is encountered. You might wish to run psql with the `ON_ERROR_STOP` variable set to alter that behavior and have psql exit with an exit status of 3 if an SQL error occurs: 
    
    
    psql -X --set ON_ERROR_STOP=on _dbname_ < _dumpfile_
    

Either way, you will only have a partially restored database. Alternatively, you can specify that the whole dump should be restored as a single transaction, so the restore is either fully completed or fully rolled back. This mode can be specified by passing the `-1` or `--single-transaction` command-line options to psql. When using this mode, be aware that even a minor error can rollback a restore that has already run for many hours. However, that might still be preferable to manually cleaning up a complex database after a partially restored dump. 

The ability of pg_dump and psql to write to or read from pipes makes it possible to dump a database directly from one server to another, for example: 
    
    
    pg_dump -h _host1_ _dbname_ | psql -X -h _host2_ _dbname_
    

### Important

The dumps produced by pg_dump are relative to `template0`. This means that any languages, procedures, etc. added via `template1` will also be dumped by pg_dump. As a result, when restoring, if you are using a customized `template1`, you must create the empty database from `template0`, as in the example above. 

After restoring a backup, it is wise to run [`ANALYZE`](sql-analyze.md "ANALYZE") on each database so the query optimizer has useful statistics; see [Section 24.1.3](routine-vacuuming.md#VACUUM-FOR-STATISTICS "24.1.3. Updating Planner Statistics") and [Section 24.1.6](routine-vacuuming.md#AUTOVACUUM "24.1.6. The Autovacuum Daemon") for more information. For more advice on how to load large amounts of data into PostgreSQL efficiently, refer to [Section 14.4](populate.md "14.4. Populating a Database"). 

### 25.1.2. Using pg_dumpall #

pg_dump dumps only a single database at a time, and it does not dump information about roles or tablespaces (because those are cluster-wide rather than per-database). To support convenient dumping of the entire contents of a database cluster, the [pg_dumpall](app-pg-dumpall.md "pg_dumpall") program is provided. pg_dumpall backs up each database in a given cluster, and also preserves cluster-wide data such as role and tablespace definitions. The basic usage of this command is: 
    
    
    pg_dumpall > _dumpfile_
    

The resulting dump can be restored with psql: 
    
    
    psql -X -f _dumpfile_ postgres
    

(Actually, you can specify any existing database name to start from, but if you are loading into an empty cluster then `postgres` should usually be used.) It is always necessary to have database superuser access when restoring a pg_dumpall dump, as that is required to restore the role and tablespace information. If you use tablespaces, make sure that the tablespace paths in the dump are appropriate for the new installation. 

pg_dumpall works by emitting commands to re-create roles, tablespaces, and empty databases, then invoking pg_dump for each database. This means that while each database will be internally consistent, the snapshots of different databases are not synchronized. 

Cluster-wide data can be dumped alone using the pg_dumpall `--globals-only` option. This is necessary to fully backup the cluster if running the pg_dump command on individual databases. 

### 25.1.3. Handling Large Databases #

Some operating systems have maximum file size limits that cause problems when creating large pg_dump output files. Fortunately, pg_dump can write to the standard output, so you can use standard Unix tools to work around this potential problem. There are several possible methods: 

**Use compressed dumps.** You can use your favorite compression program, for example gzip: 
    
    
    pg_dump _dbname_ | gzip > _filename_.gz
    

Reload with: 
    
    
    gunzip -c _filename_.gz | psql _dbname_
    

or: 
    
    
    cat _filename_.gz | gunzip | psql _dbname_
    

**Use`split`. ** The `split` command allows you to split the output into smaller files that are acceptable in size to the underlying file system. For example, to make 2 gigabyte chunks: 
    
    
    pg_dump _dbname_ | split -b 2G - _filename_
    

Reload with: 
    
    
    cat _filename_ * | psql _dbname_
    

If using GNU split, it is possible to use it and gzip together: 
    
    
    pg_dump _dbname_ | split -b 2G --filter='gzip > $FILE.gz'
    

It can be restored using `zcat`. 

**Use pg_dump's custom dump format. ** If PostgreSQL was built on a system with the zlib compression library installed, the custom dump format will compress data as it writes it to the output file. This will produce dump file sizes similar to using `gzip`, but it has the added advantage that tables can be restored selectively. The following command dumps a database using the custom dump format: 
    
    
    pg_dump -Fc _dbname_ > _filename_
    

A custom-format dump is not a script for psql, but instead must be restored with pg_restore, for example: 
    
    
    pg_restore -d _dbname_ _filename_
    

See the [pg_dump](app-pgdump.md "pg_dump") and [pg_restore](app-pgrestore.md "pg_restore") reference pages for details. 

For very large databases, you might need to combine `split` with one of the other two approaches. 

**Use pg_dump's parallel dump feature. ** To speed up the dump of a large database, you can use pg_dump's parallel mode. This will dump multiple tables at the same time. You can control the degree of parallelism with the `-j` parameter. Parallel dumps are only supported for the "directory" archive format. 
    
    
    pg_dump -j _num_ -F d -f _out.dir_ _dbname_
    

You can use `pg_restore -j` to restore a dump in parallel. This will work for any archive of either the "custom" or the "directory" archive mode, whether or not it has been created with `pg_dump -j`. 

* * *

[Prev](backup.md "Chapter 25. Backup and Restore") | [Up](backup.md "Chapter 25. Backup and Restore")|  [Next](backup-file.md "25.2. File System Level Backup")  
---|---|---  
Chapter 25. Backup and Restore | [Home](index.md "PostgreSQL 17.5 Documentation")|  25.2. File System Level Backup
