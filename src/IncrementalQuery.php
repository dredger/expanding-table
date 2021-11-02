<?php
namespace IncrementalTable;


/**
 * Created by .
 * User: dredger
 * Date: 12/3/2018
 * Time: 19:51
 */
class IncrementalQuery
{
    /**
     * @var null
     */
    public $startId        = null;
    /**
     * @var null
     */
    public $finishId       = null;
    /**
     * @var null
     */
    public $chunkSize      = null;
    /**
     * @var null
     */
    public $chunkStart     = null;
    /**
     * @var null
     */
    public $incrementDelay = null;
    /**
     * @var string primary column name
     */
    public $pkColumnName = '';
    /**
     * @var string
     */
    public $tableName = '';

    /**
     * @var DbAdapterInterface|null
     */
    private $db = null;
    /**
     * @var null|LoggerInterface
     */
    private $logger = null;

    /**@var bool true means not copy PK value to copy table , use new autoincrement instead */
    public $ignorePk;


    /**
     * DbTools constructor.
     *
     * @param DbAdapterInterface $db
     * @param LoggerInterface $logger
     */
    public function __construct(DbAdapterInterface $db,  $logger)
    {
        $this->db     = $db;
        $this->logger = $logger;
    }


    /**
     *
     */
    public function setup()
    {
        if($this->finishId<= 0)
        {
            throw new IncrementalQueryException("Finish Id must be greater than zero, current value is   [{$this->finishId} ].");
        }

        if(is_null($this->chunkSize))
        {
            $this->chunkSize = 1000;
        }

        if(is_null($this->startId))
        {
            $this->startId = 0;
        }
        if(is_null($this->incrementDelay))
        {
            $this->incrementDelay = 0;
        }
    }

    public function start(){

        $this->db->exec("DROP TEMPORARY TABLE IF EXISTS `{$this->tableName}_tmp`");
        $this->db->exec("CREATE TEMPORARY TABLE `{$this->tableName}_tmp` LIKE `{$this->tableName}_copy`");
    }

    public function finish() {
        $this->db->exec("DROP TEMPORARY TABLE `{$this->tableName}_tmp`");
    }

    public function processChunk()
    {
        $this->logger->info("Process chunk BETWEEN {$this->chunkStart} AND {$this->chunkEnd}");

        $this->getColumns();
        $this->db->exec("TRUNCATE TABLE `{$this->tableName}_tmp`");
        $this->db->exec("INSERT INTO `{$this->tableName}_tmp` (" . $this->getColumns() . ") SELECT "   . $this->getColumns()     . " FROM `{$this->tableName}` WHERE `{$this->tableName}`.`{$this->pkColumnName}` BETWEEN {$this->chunkStart} AND {$this->chunkEnd}");
        $this->db->exec("INSERT INTO `{$this->tableName}_copy` (" . $this->getColumns() . ")  SELECT " . $this->getColumns(true) . " FROM `{$this->tableName}_tmp`");
    }

    /**
     * required for case when _copy was altered before copy creation
     *
     * @param bool $forInsert
     * @return string comma separated columns from $this->table
     */
    public function getColumns($forInsert = false)
    {
        static $r = array();

        if(!empty($r[$forInsert])){
            return $r[$forInsert];
        }

        $sql = "SHOW COLUMNS FROM {$this->tableName}";
        $fields = $this->db->fetchColumn($sql, 'Field');

        if(empty($fields))
        {
            throw new IncrementalQueryException("Seems like the table [{$this->tableName} ] does not exist or it has no fields.");
        }

        if ($forInsert && $this->ignorePk)
        {
            $func = function($value, $pkName)
            {
                if($value == $pkName || $value == "`$pkName`")
                {
                    $this->logger->info("pk ignore found [$pkName]");
                    $value = 'NULL' ;
                }
                return $value;
            };
            $fields = array_map($func, $fields, array($this->pkColumnName));
        }

        $r[$forInsert] = implode(',', $fields) ;
        $this->logger->info("getColumns Finish. Copied fields forInsert[$forInsert]:   " . $r[$forInsert]);
        return $r[$forInsert];
    }


    /**
     * @return mixed
     */
    public function getLastCopiedId()
    {
        $r = $this->db->mysqlFetchField("SELECT {$this->pkColumnName} FROM {$this->tableName}_copy ORDER BY {$this->pkColumnName} DESC LIMIT 1");
        $this->logger->info("getLastCopiedId  = $r ");
        return $r;
    }

    /**
     * @return mixed
     */
    public function getItemsCount(){
        return $this->db->mysqlFetchField("SELECT {$this->pkColumnName} FROM {$this->tableName} ORDER BY {$this->pkColumnName} DESC LIMIT 1");
    }

    /**
     * @return mixed
     */
    public function getFinishId()
    {
        $r = $this->db->mysqlFetchField("SELECT {$this->pkColumnName} FROM {$this->tableName} ORDER BY {$this->pkColumnName} DESC LIMIT 1");
        $this->logger->info("getFinishId  = $r ");
        return $r;
    }

    /**
     * @return string
     */
    public function getChunkString()
    {
        return "{$this->chunkStart} - {$this->chunkEnd}";
    }

    /**
     *
     */
    public function increment()
    {
        if (is_null($this->chunkStart))
        {
            $this->chunkStart = $this->startId; // init
        }else {
            $this->chunkStart += $this->chunkSize;
        }

        $this->chunkEnd = min($this->chunkStart + $this->chunkSize - 1, $this->finishId);

        if($this->chunkStart > $this->chunkEnd){
            $this->chunkEnd = $this->chunkStart;
        }

        $this->logger->info("***** increment chunkStart= {$this->chunkStart} chunkEnd  = $this->chunkEnd ");
    }

    /**
     * @return bool
     */
    public function isFinished()
    {

        $this->logger->info("isFinished chunkStart[{$this->chunkStart}] finishId[$this->finishId]");

        return $this->chunkStart >= $this->finishId;
    }
}
