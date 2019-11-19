<?php

declare(strict_types=1);

namespace IDCT\CsvWriter\Tests;

use IDCT\CsvWriter\CsvWriter;
use IDCT\CsvWriter\DbCsvWriter;
use InvalidArgumentException;
use LogicException;
use org\bovigo\vfs\vfsStream;        
use org\bovigo\vfs\vfsStreamDirectory;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use \PDO;

final class DbCsvWriterTest extends TestCase
{
    public function testSetGetPdo()
    {
        $pdo = new PDO('sqlite::memory:');

        $dbCsvWriter = new DbCsvWriter();

        $this->assertNull($dbCsvWriter->getPdo());
        $this->assertNull($dbCsvWriter->isDbRemote());
        
        $dbCsvWriter->setPdo($pdo);
        $this->assertSame($dbCsvWriter->getPdo(), $pdo);
        $this->assertTrue($dbCsvWriter->isDbRemote());

        $dbCsvWriter->setPdo($pdo, false);
        $this->assertSame($dbCsvWriter->getPdo(), $pdo);
        $this->assertFalse($dbCsvWriter->isDbRemote());

        $dbCsvWriter->setPdo($pdo, true);
        $this->assertSame($dbCsvWriter->getPdo(), $pdo);
        $this->assertTrue($dbCsvWriter->isDbRemote());
    }

    public function testGetSetTempdir()
    {
        $sysTempDir = sys_get_temp_dir();
        $sysTempDirSlash = $sysTempDir . DIRECTORY_SEPARATOR;
        
        $dbCsvWriter = new DbCsvWriter();
        $this->assertEquals($dbCsvWriter->getTmpDir(), $sysTempDirSlash);

        $fileSystemMock = vfsStream::setup('sampleDir');
        $tmpdir = $fileSystemMock->url('sampleDir');
        $tmpdirSlash = $tmpdir . DIRECTORY_SEPARATOR;

        $dbCsvWriter->setTmpDir($tmpdir);
        $this->assertEquals($dbCsvWriter->getTmpDir(), $tmpdirSlash);

        $dbCsvWriter->setTmpDir($tmpdirSlash);
        $this->assertEquals($dbCsvWriter->getTmpDir(), $tmpdirSlash);
    }

    public function testGetSetTempdirInvalidDir()
    {
        $dbCsvWriter = new DbCsvWriter();
        $this->expectException(RuntimeException::class);

        $fileSystemMock = vfsStream::setup('sampleDir');
        $tmpdir = $fileSystemMock->url('sampleDir') . DIRECTORY_SEPARATOR . 'invalid'; 

        $dbCsvWriter->setTmpDir($tmpdir);
    }

    public function testGetSetTempdirDirNotWritable()
    {
        $dbCsvWriter = new DbCsvWriter();
        $this->expectException(RuntimeException::class);

        $fileSystemMock = vfsStream::setup('sampleDir');            
        $fileSystemMock->chmod(0400);
        $dbCsvWriter->setTmpDir($fileSystemMock->url('sampleDir'));
    }

    public function testAppendDataNoCollection()
    {
        $dbCsvWriter = new DbCsvWriter();
        $this->expectException(RuntimeException::class);
        $dbCsvWriter->appendData(['aa','bb']);
    }

    public function testSetGetBuffer()
    {
        $dbCsvWriter = new DbCsvWriter();

        $dbCsvWriter->setBufferSize(11);
        $this->assertEquals(11, $dbCsvWriter->getBufferSize());
    }


    public function testAppendData()
    {

        $dbCsvWriter = $this->getMockBuilder(DbCsvWriter::class)
        ->setMethods(['getCsvWriter', 'hasOpenCollection', 'escape'])
        ->getMock();

        $csvWriter = $this->createMock(CsvWriter::class);
        $csvWriter->expects($this->once())
            ->method('write')
            ->willReturn($csvWriter);

        $dbCsvWriter->expects($this->once())
            ->method('getCsvWriter')
            ->willReturn($csvWriter);

        $dbCsvWriter->expects($this->exactly(2))
            ->method('escape')
            ->will($this->onConsecutiveCalls('aa', 'bb'));

        $dbCsvWriter->method('hasOpenCollection')
            ->willReturn(true);

        $dbCsvWriter->appendData(['aa','bb']);

        //any assertion here would require actual CsvWriter verification
    }

    public function testStartCollectionInvalidCollectionName()
    {
        $dbCsvWriter = new DbCsvWriter();
        $this->expectException(InvalidArgumentException::class);
        $dbCsvWriter->startCollection('', ['aa','bb']);
    }

    public function testStartCollectionInvalidCollectionName2()
    {
        $dbCsvWriter = new DbCsvWriter();
        $this->expectException(InvalidArgumentException::class);
        $dbCsvWriter->startCollection('$%', ['aa','bb']);
    }

    public function testStartCollectionInvalidMissingFields()
    {
        $dbCsvWriter = new DbCsvWriter();
        $this->expectException(InvalidArgumentException::class);
        $dbCsvWriter->startCollection('aa',[]);
    }

    public function testStartCollectionInvalidInvalidFields()
    {
        $dbCsvWriter = new DbCsvWriter();
        $this->expectException(InvalidArgumentException::class);
        $dbCsvWriter->startCollection('aa',['aa','a$b']);
    }

    public function testStartCollection()
    {
        $dbCsvWriter = $this->getMockBuilder(DbCsvWriter::class)
        ->setMethods(['getCsvWriter'])
        ->getMock();

        $csvWriter = $this->createMock(CsvWriter::class);
        $csvWriter->expects($this->once())
            ->method('write')
            ->willReturn($csvWriter);

        $csvWriter->expects($this->once())
            ->method('open')
            ->willReturn($csvWriter);            

        $dbCsvWriter->expects($this->once())
            ->method('getCsvWriter')
            ->willReturn($csvWriter);

        $this->assertFalse($dbCsvWriter->hasOpenCollection());
        $dbCsvWriter->startCollection('aaa', ['aa','bb']);
        $this->assertTrue($dbCsvWriter->hasOpenCollection());

        //any assertion here would require actual CsvWriter verification
    }

    public function testOpenCollectionMissing()
    {
        $dbCsvWriter = new DbCsvWriter();
        $this->expectException(RuntimeException::class);
        $dbCsvWriter->openCollection('aaa');
    }

    public function testOpenCollectionFullPath()
    {
        //prepare colleciton
        $dbCsvWriter = new DbCsvWriter();
        $pdo = new PDOExecTest();

        $dbCsvWriter->setPdo($pdo);
        
        $fileSystemMock = vfsStream::setup('sampleDir');
        $tmpdir = $fileSystemMock->url('sampleDir');
        $tmpdirSlash = $tmpdir . DIRECTORY_SEPARATOR;

        $dbCsvWriter->setTmpDir($tmpdir);
        $dbCsvWriter->startCollection('aaa', ['aa','bb']);
        $dbCsvWriter->appendData(['data\\1_1', 'da\\ta\\1_2']);
        $dbCsvWriter->appendData(['data\\2_2', 'da\\ta\\2_2']);
        $dbCsvWriter->storeCollection('aaa');
        $dbCsvWriter->closeCollection(true);

        //open via full path
        $dbCsvWriterSecond = new DbCsvWriter();
        $dbCsvWriterSecond->openCollection($tmpdirSlash . 'aaa.csv');
        $this->assertEquals($dbCsvWriterSecond->getOpenCollectionsPath(), $tmpdirSlash . 'aaa.csv');
    }

    public function testOpenCollectionInvalid()
    {
        $dbCsvWriter = new DbCsvWriter();
        $this->expectException(InvalidArgumentException::class);
        $dbCsvWriter->openCollection('');
    }

    public function testOpenCollectionInvalid2()
    {
        $dbCsvWriter = new DbCsvWriter();
        $this->expectException(InvalidArgumentException::class);
        $dbCsvWriter->openCollection('$');
    }

    public function testCloseCollectionNoCollection()
    {
        $dbCsvWriter = new DbCsvWriter();
        $this->assertSame($dbCsvWriter, $dbCsvWriter->closeCollection());
    }
    
    public function testCloseCollectionOpenCollectionKeepAttached()
    {
        $dbCsvWriter = new DbCsvWriter();

        $dbCsvWriter = $this->getMockBuilder(DbCsvWriter::class)
        ->setMethods(['getCsvWriter'])
        ->getMock();
    
        $csvWriter = $this->createMock(CsvWriter::class);
        $csvWriter->expects($this->once())
            ->method('write')
            ->willReturn($csvWriter);

        $csvWriter->expects($this->once())
            ->method('open')
            ->willReturn($csvWriter);

        $dbCsvWriter->expects($this->any())
            ->method('getCsvWriter')
            ->willReturn($csvWriter);            

        $fileSystemMock = vfsStream::setup('sampleDir');
        $tmpdir = $fileSystemMock->url('sampleDir');
        $tmpdirSlash = $tmpdir . DIRECTORY_SEPARATOR;

        $dbCsvWriter->setTmpDir($tmpdir);
        $dbCsvWriter->startCollection('aaa', ['aa','bb']);
        $this->assertTrue($dbCsvWriter->hasOpenCollection());
        $this->assertEquals($tmpdir . DIRECTORY_SEPARATOR . 'aaa.csv', $dbCsvWriter->getOpenCollectionsPath());
        $dbCsvWriter->closeCollection();
        $this->assertFalse($dbCsvWriter->hasOpenCollection());
        $this->assertEquals($tmpdir . DIRECTORY_SEPARATOR . 'aaa.csv', $dbCsvWriter->getOpenCollectionsPath());
    }

    public function testCloseCollectionOpenCollectionDetach()
    {
        $dbCsvWriter = new DbCsvWriter();

        $dbCsvWriter = $this->getMockBuilder(DbCsvWriter::class)
        ->setMethods(['getCsvWriter'])
        ->getMock();
    
        $csvWriter = $this->createMock(CsvWriter::class);
        $csvWriter->expects($this->once())
            ->method('write')
            ->willReturn($csvWriter);

        $csvWriter->expects($this->once())
            ->method('open')
            ->willReturn($csvWriter);

        $dbCsvWriter->expects($this->any())
            ->method('getCsvWriter')
            ->willReturn($csvWriter);            

        $fileSystemMock = vfsStream::setup('sampleDir');
        $tmpdir = $fileSystemMock->url('sampleDir');
        $tmpdirSlash = $tmpdir . DIRECTORY_SEPARATOR;

        $dbCsvWriter->setTmpDir($tmpdir);
        $dbCsvWriter->startCollection('aaa', ['aa','bb']);
        $this->assertTrue($dbCsvWriter->hasOpenCollection());
        $this->assertEquals($tmpdir . DIRECTORY_SEPARATOR . 'aaa.csv', $dbCsvWriter->getOpenCollectionsPath());
        $dbCsvWriter->closeCollection(true);
        $this->assertFalse($dbCsvWriter->hasOpenCollection());
        $this->assertNull($dbCsvWriter->getOpenCollectionsPath());
    }    

    public function testRemoveCollectionError()
    {
        $this->expectException(RuntimeException::class);
        $dbCsvWriter = new DbCsvWriter();
        $dbCsvWriter->removeCollection();
    }

    public function testRemoveCollection()
    {
        $dbCsvWriter = new DbCsvWriter();

        $dbCsvWriter = $this->getMockBuilder(DbCsvWriter::class)
        ->setMethods(['closeCollection'])
        ->getMock();

        $dbCsvWriter->expects($this->once())
            ->method('closeCollection')
            ->willReturn($dbCsvWriter);
    /*
        $csvWriter = $this->createMock(CsvWriter::class);
        $csvWriter->expects($this->once())
            ->method('write')
            ->willReturn($csvWriter);

        $csvWriter->expects($this->once())
            ->method('open')
            ->willReturn($csvWriter);

        $dbCsvWriter->expects($this->any())
            ->method('getCsvWriter')
            ->willReturn($csvWriter);         
*/
        $fileSystemMock = vfsStream::setup('sampleDir');
        $tmpdir = $fileSystemMock->url('sampleDir');
        $tmpdirSlash = $tmpdir . DIRECTORY_SEPARATOR;

        $dbCsvWriter->setTmpDir($tmpdir);
        $dbCsvWriter->startCollection('aaa', ['aa','bb']);
        $file = $tmpdirSlash . 'aaa.csv';
        $this->assertTrue(file_exists($file));
        $this->assertTrue($dbCsvWriter->hasOpenCollection());
        $this->assertEquals($tmpdir . DIRECTORY_SEPARATOR . 'aaa.csv', $dbCsvWriter->getOpenCollectionsPath());
        $dbCsvWriter->removeCollection();

        $this->assertFalse(file_exists($file));
        $this->assertFalse($dbCsvWriter->hasOpenCollection());
        $this->assertNull($dbCsvWriter->getOpenCollectionsPath());
    }

    public function testStoreCollectionError()
    {
        $this->expectException(RuntimeException::class);
        $dbCsvWriter = new DbCsvWriter();
        $dbCsvWriter->storeCollection('any');
    }

    public function testStoreCollectionPdoError()
    {
        $dbCsvWriter = $this->getMockBuilder(DbCsvWriter::class)
        ->setMethods(['hasOpenCollection'])
        ->getMock();

        $dbCsvWriter->expects($this->once())
        ->method('hasOpenCollection')
        ->willReturn(true);

        $this->expectException(RuntimeException::class);
        $dbCsvWriter->storeCollection('any');
    }

    public function testStoreCollection()
    {
        $dbCsvWriter = $this->getMockBuilder(DbCsvWriter::class)
        ->setMethods(['hasOpenCollection', 'closeCollection', 'saveFromCsvFile', 'getPdo'])
        ->getMock();

        $pdo = $this->createMock(PDO::class);

        $fileSystemMock = vfsStream::setup('sampleDir');
        $tmpdir = $fileSystemMock->url('sampleDir');
        $tmpdirSlash = $tmpdir . DIRECTORY_SEPARATOR;

        $dbCsvWriter->setTmpDir($tmpdir);
        $dbCsvWriter->startCollection('aaa', ['aa','bb']);

        $dbCsvWriter->expects($this->once())
            ->method('hasOpenCollection')
            ->willReturn(true);

            $dbCsvWriter->expects($this->once())
            ->method('getPdo')
            ->willReturn($pdo);

            $dbCsvWriter->expects($this->once())
            ->method('closeCollection')
            ->willReturn($dbCsvWriter);

            $dbCsvWriter->expects($this->once())
            ->method('saveFromCsvFile')
            ->willReturn(5);

        $dbCsvWriter->storeCollection('aaa');

        $this->assertEquals(5, $dbCsvWriter->getLastResultCount());
    }

    public function testStoreCollectionActualExport()
    {
        $dbCsvWriter = new DbCsvWriter();
        $pdo = new PDOExecTest();

        $dbCsvWriter->setPdo($pdo);
        
        $fileSystemMock = vfsStream::setup('sampleDir');
        $tmpdir = $fileSystemMock->url('sampleDir');
        $tmpdirSlash = $tmpdir . DIRECTORY_SEPARATOR;

        $dbCsvWriter->setTmpDir($tmpdir);
        $dbCsvWriter->startCollection('aaa', ['aa','bb']);
        $dbCsvWriter->appendData(['data\\1_1', 'da\\ta\\1_2']);
        $dbCsvWriter->appendData(['data\\2_2', 'da\\ta\\2_2']);
        $dbCsvWriter->storeCollection('aaa');

        $this->assertEquals(55, $dbCsvWriter->getLastResultCount());
        $expected = <<<HEREDATA
LOAD DATA LOW_PRIORITY LOCAL INFILE "vfs://sampleDir/aaa.csv"
INTO TABLE aaa
CHARACTER SET utf8
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`aa`,`bb`)
HEREDATA;

        $dbCsvWriter->setPdo($pdo, false);

    $expected = <<<HEREDATA
LOAD DATA LOW_PRIORITY INFILE "vfs://sampleDir/bbb.csv"
INTO TABLE bbb
CHARACTER SET utf8
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(`aa`,`bb`)
HEREDATA;

        $dbCsvWriter->startCollection('bbb', ['aa','bb']);
        $dbCsvWriter->appendData(['data\\1_1', 'da\\ta\\1_2']);
        $dbCsvWriter->appendData(['data\\2_2', 'da\\ta\\2_2']);
        $dbCsvWriter->storeCollection('bbb');
        
        $expectedFileContents = "aa,bb\n" . 
        "\"data\\\\1_1\",\"da\\\\ta\\\\1_2\"\n" .
        ("\"data\\\\2_2\",\"da\\\\ta\\\\2_2\"\n");

        $this->assertEquals(\file_get_contents($tmpdirSlash . 'bbb.csv'), $expectedFileContents); //tests escape
        $this->assertEquals($expected, $pdo->lastQuery);


    }

/*
    public function testOpenCollection()
    {
        $dbCsvWriter = $this->getMockBuilder(DbCsvWriter::class)
        ->setMethods(['getCsvWriter'])
        ->getMock();

        $csvWriter = $this->createMock(CsvWriter::class);
        $csvWriter->expects($this->once())
            ->method('write')
            ->willReturn($csvWriter);

        $csvWriter->expects($this->once())
            ->method('open')
            ->willReturn($csvWriter);            

        $dbCsvWriter->expects($this->once())
            ->method('getCsvWriter')
            ->willReturn($csvWriter);

        $this->assertFalse($dbCsvWriter->hasOpenCollection());
        $dbCsvWriter->startCollection('aaa', ['aa','bb']);
        $this->assertTrue($dbCsvWriter->hasOpenCollection());

        //any assertion here would require actual CsvWriter verification
    }    */
}