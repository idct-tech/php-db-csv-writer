<?php

namespace IDCT\CsvWriter;

use InvalidArgumentException;
use PDO;
use RuntimeException;

/**
 * DbCsvWriter allows buffered Csv data to be loaded easily into target database
 * tables.
 */
class DbCsvWriter
{
    /**
     * Database connection instance.
     *
     * @var PDO
     */
    protected $pdo;

    /**
     * Temporary folder's path.
     *
     * @var string
     */
    protected $tmpDir;

    /**
     * CsvWriter's instance.
     *
     * @var CsvWriter
     */
    protected $csvWriter;

    /**
     * Path to the last (properly) assigned collection.
     *
     * @var string
     */
    protected $lastCollection;

    /**
     * Internal which informs if we have a collection opened.
     *
     * @var boolean
     */
    protected $opened;

    /**
     * If set to true then "LOCAL" string will be added to the LOAD INFILE
     * command so that database engine knows it is running on a different machine
     * than call was made.
     *
     * @var boolean
     */
    protected $isRemote;

    /**
     * Number of results loaded into the database during last query.
     * Null is returned until any query is executed.
     *
     * @var int|null
     */
    protected $lastResultsCount;

    /**
     * Creates instance of the new DbCsvWriter with required settings and
     * components.
     */
    public function __construct()
    {
        $this->csvWriter = new CsvWriter();
        $this->csvWriter->setEolSymbol(CsvWriter::EOL_LINUX);
        $this->opened = false;
    }

    /**
     * Gets the absolute full path to the opened collection or null.
     */
    public function getOpenCollectionsPath(): ?string
    {
        return $this->lastCollection;
    }

    /**
     * Sets the instance of the database connection.
     * By default assumes that the database is remote to the working terminal
     * machine (on which application runs) so adds the "LOCAL" string into the
     * LOAD INFILE command. To avoid that please set $isRemote to true: this will
     * make the database load the file on the same server as it is running.
     *
     * @var PDO $pdo
     * @var boolean $isRemote
     * @return $this
     */
    public function setPdo(PDO $pdo, $isRemote = true): self
    {
        $this->pdo = $pdo;
        $this->isRemote = $isRemote;

        return $this;
    }

    /**
     * Returns the database connection instance (pdo).
     *
     * @return PDO
     */
    public function getPdo(): ?PDO
    {
        return $this->pdo;
    }

    /**
     * Returns the value of the "$isRemote" variable which answers if client
     * connection is remote to the db server.
     *
     * Used to force loading of local files.
     *
     * @var boolean|null
     */
    public function isDbRemote(): ?bool
    {
        return $this->isRemote;
    }

    /**
     * Gets the buffer size in bytes (defaults to 0).
     *
     * @return int
     */
    public function getBufferSize(): int
    {
        return $this->csvWriter->getBufferSize();
    }

    /**
     * Sets the buffer size. In bytes.
     * If file is already open the attempts to modify the stream.
     *
     * @param int|null $bufferSize
     * @throws InvalidArgumentException
     * @return $this
     */
    public function setBufferSize($bufferSize): self
    {
        $this->csvWriter->setBufferSize($bufferSize);

        return $this;
    }

    /**
     * Sets the temporary storage folder's path.
     *
     * @var string $path
     * @return $this
     */
    public function setTmpDir(string $path): self
    {
        if (!is_writable($path) || !is_dir($path)) {
            throw new RuntimeException("Temporary folder must be a writable directory!");
        }

        //ensures trailing [back]slash
        if (substr($path, -1, 1) !== DIRECTORY_SEPARATOR) {
            $path .= DIRECTORY_SEPARATOR;
        }

        $this->tmpDir = $path;

        return $this;
    }

    /**
     * Returns path to the previously assigned temporary storage folder.
     *
     * @return string
     */
    public function getTmpDir(): string
    {
        if ($this->tmpDir === null) {
            return sys_get_temp_dir() . DIRECTORY_SEPARATOR;
        }

        return $this->tmpDir;
    }

    /**
     * Answers if any collection is opened.
     *
     * @return boolean
     */
    public function hasOpenCollection(): bool
    {
        return $this->opened;
    }

    /**
     * Writes data array to the opened collection.
     * @var array $data
     * @todo check fields count
     * @throws RuntimeException
     * @return $this
     */
    public function appendData(array $data): self
    {
        if (!$this->hasOpenCollection()) {
            throw new RuntimeException("No collection opened.");
        }

        $csvWriter = $this->getCsvWriter();
        foreach ($data as $key => $element) {
            $data[$key] = $this->escape($element);
        }
        $csvWriter->write($data);

        return $this;
    }

    /**
     * Creates a new collection (new csv file) and writes field names as the first
     * list. Keeps handle to the file opened.
     *
     * @var string $name
     * @var string[] $fields
     * @throws InvalidArugmentException
     * @return $this
     */
    public function startCollection(string $name, array $fields): self
    {
        if (empty($name) || !preg_match('/^[0-9a-z\-_]+$/i', $name)) {
            throw new InvalidArgumentException('Collection name must non-empty string consisting only letters, numbers and - _ symbols.');
        }

        if (empty($fields)) {
            throw new InvalidArgumentException('Fields names must be non-empty array of strings.');
        }

        foreach ($fields as $field) {
            if (!preg_match('/^[A-Za-z][A-Za-z0-9_]+$/i', $field)) {
                throw new InvalidArgumentException('Invalid field name: `' . $field . '`.');
            }
        }

        $csvWriter = $this->getCsvWriter();
        $tmpDir = $this->getTmpDir();
        $this->lastCollection = $tmpDir . $name . '.csv';
        $csvWriter->open($this->lastCollection)
                  ->write($fields)
                ;

        $this->opened = true;

        return $this;
    }

    /**
     * Opens collection in the temporary storage folder by it's name (without
     * the .csv extension).
     *
     * Closes any previously opened collection.
     *
     * @var $name Collection's name (csv file without .csv)
     * @return $this
     * @throws InvalidArgumentException
     * @throws RuntimeException
     */
    public function openCollection(string $name): self
    {
        if (empty($name)) {
            throw new InvalidArgumentException('Collection name cannot be empty.');
        }

        if (strtolower(substr($name, -4, 4)) === ".csv") {
            //already full path
            $path = $name;
        } else {
            if (!preg_match('/^[0-9a-z\-_]+$/i', $name)) {
                throw new InvalidArgumentException('Collection name must non-empty string consisting only letters, numbers and - _ symbols.');
            }
            $path = $this->getTmpDir() . $name . '.csv';
        }

        if (!file_exists($path) || !is_readable($path)) {
            throw new RuntimeException("Collection " . $path . " not readable!");
        }

        $csvWriter = $this->getCsvWriter();
        $csvWriter->open($path, CsvWriter::FILEMODE_APPEND);
        $this->lastCollection = $path;
        $this->opened = true;

        return $this;
    }

    /**
     * Closes the collection (and flushes the buffer). Keeps the collectin assigned
     * to the DbCsvWriter (for store and remove methods) unless $detach is set
     * to true.
     *
     * @var boolean $detach
     * @return $this
     */
    public function closeCollection(bool $detach = false): self
    {
        if ($this->hasOpenCollection()) {
            $this->getCsvWriter()->close();
            $this->opened = false;
            if ($detach) {
                $this->lastCollection = null;
            }
        }

        return $this;
    }

    /**
     * Closes the opened collection and removes it's file.
     *
     * @throws RuntimeException
     * @return $this
     */
    public function removeCollection(): self
    {
        if (!$this->hasOpenCollection()) {
            throw new RuntimeException('No open collection.');
        }

        $this->closeCollection();
        unlink($this->lastCollection);
        $this->lastCollection = null;
        $this->opened = false;

        return $this;
    }

    /**
     * Closes the opened collection and attempts to store it (save into the
     * database) into the provided database.
     *
     * On success records the last results count.
     *
     * @var string $tableName
     * @throws RuntimeException
     * @return $this
     */
    public function storeCollection(string $tableName): self
    {
        if (!$this->hasOpenCollection()) {
            throw new RuntimeException('No open collection.');
        }

        if (!($this->getPdo() instanceof PDO)) {
            throw new RuntimeException('Missing db connection: assign PDO using setPdo(PDO $pdo).');
        }

        $this->closeCollection();
        $this->lastResultsCount = $this->saveFromCsvFile($this->lastCollection, $tableName);

        return $this;
    }

    /**
     * Returns last operation's results count.
     *
     * @return int|null
     */
    public function getLastResultCount(): ?int
    {
        return $this->lastResultsCount;
    }

    /**
     * Escapes slashes in the text.
     *
     * @param string
     * @return string
     */
    protected function escape(string $string): string
    {
        $string = str_replace('\\', '\\\\', $string);

        return $string;
    }

    /**
     * Returns handle to the internal csv writer.
     *
     * @var CsvWriter
     */
    protected function getCsvWriter(): CsvWriter
    {
        return $this->csvWriter;
    }

    /**
     * Retrieves field names from first line of a CSV file.
     *
     * @param string $csvFile (full file path)
     * @return string
     */
    protected function getSqlFieldStringFromCsvFile(string $csvFile): string
    {
        $fields = fgetcsv($handle = fopen($csvFile, 'r'));
        fclose($handle);

        array_walk($fields, function ($field, $key) use (&$fields) {
            $fields[$key] = "`$field`";
        });

        return join(',', $fields);
    }

    /**
     * Internal method for saving the csv file into the database.
     *
     * @var string $csvFile Path to the file
     * @var string $tableName Database table name
     * @return int Results added count
     * @throws PDOException
     */
    protected function saveFromCsvFile(string $csvFile, string $tableName): int
    {
        // import data from csv
        $fields = $this->getSqlFieldStringFromCsvFile($csvFile);

        $filePosition = ' ';
        if ($this->IsDbRemote()) {
            $filePosition = ' LOCAL ';
        }

        $statement = <<<HEREDATA
LOAD DATA LOW_PRIORITY{$filePosition}INFILE "{$csvFile}"
INTO TABLE {$tableName}
CHARACTER SET utf8
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
({$fields})
HEREDATA;

        return $this->getPdo()->exec($statement);
    }
}
