CREATE COLLATION  
---  
[Prev](sql-createcast.md "CREATE CAST") | [Up](sql-commands.md "SQL Commands")| SQL Commands| [Home](index.md "PostgreSQL 17.5 Documentation")|  [Next](sql-createconversion.md "CREATE CONVERSION")  
  
* * *

## CREATE COLLATION

CREATE COLLATION — define a new collation

## Synopsis
    
    
    CREATE COLLATION [ IF NOT EXISTS ] _name_ (
        [ LOCALE = _locale_ , ]
        [ LC_COLLATE = _lc_collate_ , ]
        [ LC_CTYPE = _lc_ctype_ , ]
        [ PROVIDER = _provider_ , ]
        [ DETERMINISTIC = _boolean_ , ]
        [ RULES = _rules_ , ]
        [ VERSION = _version_ ]
    )
    CREATE COLLATION [ IF NOT EXISTS ] _name_ FROM _existing_collation_
    

## Description

`CREATE COLLATION` defines a new collation using the specified operating system locale settings, or by copying an existing collation. 

To be able to create a collation, you must have `CREATE` privilege on the destination schema. 

## Parameters

`IF NOT EXISTS`
    

Do not throw an error if a collation with the same name already exists. A notice is issued in this case. Note that there is no guarantee that the existing collation is anything like the one that would have been created. 

_`name`_
    

The name of the collation. The collation name can be schema-qualified. If it is not, the collation is defined in the current schema. The collation name must be unique within that schema. (The system catalogs can contain collations with the same name for other encodings, but these are ignored if the database encoding does not match.) 

_`locale`_
    

The locale name for this collation. See [Section 23.2.2.3.1](collation.md#COLLATION-MANAGING-CREATE-LIBC "23.2.2.3.1. libc Collations") and [Section 23.2.2.3.2](collation.md#COLLATION-MANAGING-CREATE-ICU "23.2.2.3.2. ICU Collations") for details. 

If _`provider`_ is `libc`, this is a shortcut for setting `LC_COLLATE` and `LC_CTYPE` at once. If you specify _`locale`_ , you cannot specify either of those parameters. 

If _`provider`_ is `builtin`, then _`locale`_ must be specified and set to either `C` or `C.UTF-8`. 

_`lc_collate`_
    

If _`provider`_ is `libc`, use the specified operating system locale for the `LC_COLLATE` locale category. 

_`lc_ctype`_
    

If _`provider`_ is `libc`, use the specified operating system locale for the `LC_CTYPE` locale category. 

_`provider`_
    

Specifies the provider to use for locale services associated with this collation. Possible values are `builtin`, `icu` (if the server was built with ICU support) or `libc`. `libc` is the default. See [Section 23.1.4](locale.md#LOCALE-PROVIDERS "23.1.4. Locale Providers") for details. 

`DETERMINISTIC`
    

Specifies whether the collation should use deterministic comparisons. The default is true. A deterministic comparison considers strings that are not byte-wise equal to be unequal even if they are considered logically equal by the comparison. PostgreSQL breaks ties using a byte-wise comparison. Comparison that is not deterministic can make the collation be, say, case- or accent-insensitive. For that, you need to choose an appropriate `LOCALE` setting _and_ set the collation to not deterministic here. 

Nondeterministic collations are only supported with the ICU provider. 

_`rules`_
    

Specifies additional collation rules to customize the behavior of the collation. This is supported for ICU only. See [Section 23.2.3.4](collation.md#ICU-TAILORING-RULES "23.2.3.4. ICU Tailoring Rules") for details. 

_`version`_
    

Specifies the version string to store with the collation. Normally, this should be omitted, which will cause the version to be computed from the actual version of the collation as provided by the operating system. This option is intended to be used by `pg_upgrade` for copying the version from an existing installation. 

See also [ALTER COLLATION](sql-altercollation.md "ALTER COLLATION") for how to handle collation version mismatches. 

_`existing_collation`_
    

The name of an existing collation to copy. The new collation will have the same properties as the existing one, but it will be an independent object. 

## Notes

`CREATE COLLATION` takes a `SHARE ROW EXCLUSIVE` lock, which is self-conflicting, on the `pg_collation` system catalog, so only one `CREATE COLLATION` command can run at a time. 

Use `DROP COLLATION` to remove user-defined collations. 

See [Section 23.2.2.3](collation.md#COLLATION-CREATE "23.2.2.3. Creating New Collation Objects") for more information on how to create collations. 

When using the `libc` collation provider, the locale must be applicable to the current database encoding. See [CREATE DATABASE](sql-createdatabase.md "CREATE DATABASE") for the precise rules. 

## Examples

To create a collation from the operating system locale `fr_FR.utf8` (assuming the current database encoding is `UTF8`): 
    
    
    CREATE COLLATION french (locale = 'fr_FR.utf8');
    

To create a collation using the ICU provider using German phone book sort order: 
    
    
    CREATE COLLATION german_phonebook (provider = icu, locale = 'de-u-co-phonebk');
    

To create a collation using the ICU provider, based on the root ICU locale, with custom rules: 
    
    
    CREATE COLLATION custom (provider = icu, locale = 'und', rules = '&V << w <<< W');
    

See [Section 23.2.3.4](collation.md#ICU-TAILORING-RULES "23.2.3.4. ICU Tailoring Rules") for further details and examples on the rules syntax. 

To create a collation from an existing collation: 
    
    
    CREATE COLLATION german FROM "de_DE";
    

This can be convenient to be able to use operating-system-independent collation names in applications. 

## Compatibility

There is a `CREATE COLLATION` statement in the SQL standard, but it is limited to copying an existing collation. The syntax to create a new collation is a PostgreSQL extension. 

## See Also

[ALTER COLLATION](sql-altercollation.md "ALTER COLLATION"), [DROP COLLATION](sql-dropcollation.md "DROP COLLATION")

* * *

[Prev](sql-createcast.md "CREATE CAST") | [Up](sql-commands.md "SQL Commands")|  [Next](sql-createconversion.md "CREATE CONVERSION")  
---|---|---  
CREATE CAST | [Home](index.md "PostgreSQL 17.5 Documentation")|  CREATE CONVERSION
