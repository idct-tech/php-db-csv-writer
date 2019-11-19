<?php

namespace IDCT\CsvWriter\Tests;

use PDO;

class PDOExecTest extends PDO
{
    public $lastQuery;

    public function __construct()
    {
    }

    public function exec($query)
    {
        $this->lastQuery = $query;
        return 55;
    }
}
