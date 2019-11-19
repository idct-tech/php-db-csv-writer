# PHP DB CSV Writer

PHP library which makes the process of CSV data files imports into MySQL easier.

![Tests status](https://github.com/ideaconnect/php-db-csv-writer/workflows/All%20tests%20using%20PHPUnit/badge.svg) [![Coverage Status](https://coveralls.io/repos/github/ideaconnect/php-db-csv-writer/badge.svg?branch=master)](https://coveralls.io/github/ideaconnect/php-db-csv-writer?branch=master) ![GitHub tag (latest SemVer)](https://img.shields.io/github/v/tag/ideaconnect/php-db-csv-writer?label=latest%20version&sort=semver)

Allows easy building of ready-to-import CSV collections for MySQL-compatible (like MariaDB etc.) PDO-accessible database connections.

You can add new data entries into the collection by calling `appendData(array)` and later load the file into the database by calling `storeCollection`.

## Installation
The best way to install the library in your project is by using **Composer**:
```bash
composer require idct/php-db-csv-writer
```
of course you can still manually include all the required files in your project using `using` statements yet **Composer** and autoloading is more than suggested.
## Usage
Create an instance:
```php
use IDCT\CsvWriter\DbCsvWriter;
$dbCsvWriter = new DbCsvWriter();
```

Assign a valid MySQL-compatible PDO connection usign `setPdo` method.

As there are files created you need to assign a temporary storage folder using `setTmpDir` method. If you will not specify any then system's temp directory shall be used.

### Starting a new collection

Use the `startCollection` method. It takes collection name as first argument and fields as the second. Colllection name is a handle for you: it does not need to match any table's name etc.; fields array must match fields' names in the future target table.

```php
$dbCsvWriter->startCollection('firstcollection',['fieldA', 'fieldB']);
```

This will create a file `firstcollection.csv` in the storage directory set before. Any previously opened collection will be closed.

### Opening a collection

To assign a previously created collection to the instance use `openCollection`:

```php
$dbCsvWriter->openCollection($name);
```

Where name can be a handle to a collection in the storage directory (for example it would `firstcollection` if you would like to open the collection from the previous point). It can also be a full path to a csv file.

### Closing and removing the collection

To close the collection without storing use `closeCollection` method. If you call it without any arguments:
```php
$dbCsvWriter->closeCollection();
``` 

then collection will remain assigned which further allows to remove it easily by calling
```php
$dbCsvWriter->removeCollection();
``` 

If you first detach it by calling:
```php
$dbCsvWriter->closeCollection(true);
``` 

then you cannot remove it afterwards.

### Storing the collection in the database

To save (load into the database) the collection in the database call:
```
$dbCsvWriter->storeCollection();
```

It will automatically close the collection (which flushes data into the file) and invoke `LOAD DATA INFILE` command on the databaes with the currently assigned collection.

**Warning:** by default it uses the **LOCAL** attribute in the query which informs the connector that file is stored on client's side (where the invoking computer is). If for some reason you want to load a file which is on an remote database server then specify the second argument during PDO assignment:
```php
$dbCsvWriter->setPdo($pdo, false);
```

Verify that with `isDbRemote` method.

### Buffering 

DbCsvWriter can use in-memory data buffering before saving in the CSV file. Check `setBufferSize` and `getBufferSize` methods which are wrappers over respective methods of the `CsvWriter`.

## TODO / Contribution

At the moment the main requirement is to provide better unit tests and documentation, yet if you find any bugs or have potential feature ideas then please use Issues or Pull Requests, it is more than welcome! I will try to reply ASAP.
