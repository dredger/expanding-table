<?php
namespace IncrementalTable;


/**
 * Created by .
 * User: dredger
 * Date: 12/3/2018
 * Time: 19:51
 */
class IncrementalDeleteQuery
{
    /**
     * @var null
     */
    public $startId        = 1;
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
    public $chunkEnd     = null;
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

    public function finish() {
        $this->logger->info("-- FINISH ---");
    }

    /**
     * @return string
     */
    public function getChunkString() {
        return "{$this->chunkStart} - {$this->chunkEnd}";
    }

   /**
     *
     */
    public function increment()
    {
        if (empty($this->chunkStart))
        {
            $this->chunkStart = $this->startId; // init
        } else {
            $this->chunkStart += $this->chunkSize;
        }

        $this->chunkEnd = min($this->chunkStart + $this->chunkSize - 1, $this->finishId);
    }


    /**
     * required for case when  was altered before copy creation
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

        $r[$forInsert] = implode(',', $fields) ;
        $this->logger->info("getColumns Finish. Copied fields forInsert[$forInsert]:   " . $r[$forInsert]);
        return $r[$forInsert];
    }


    /**
     * @return mixed
     */
    public function getLastId()
    {
        $r = $this->db->mysqlFetchField("SELECT {$this->pkColumnName} FROM {$this->tableName} ORDER BY {$this->pkColumnName} DESC LIMIT 1");
        $this->logger->info("LastId  = $r ");
        return $r;
    }

    /**
     * @return mixed
     */
    public function getFirstId()
    {
        $r = $this->db->mysqlFetchField("SELECT {$this->pkColumnName} FROM {$this->tableName} ORDER BY {$this->pkColumnName} ASC LIMIT 1");
        $this->logger->info("LastId  = $r ");
        return $r;
    }

    /**
     * @return bool
     */
    public function isFinished()
    {
        $this->logger->info("isFinished  start = $this->chunkStart finish ID $this->finishId ") ;

        return $this->chunkStart >= $this->finishId;
    }


    public function processChunk()
    {
        $sql = "DELETE FROM {$this->tableName} WHERE {$this->pkColumnName} BETWEEN {$this->chunkStart} AND {$this->chunkEnd}";

        // Real exec
        $this->db->exec($sql);
    }


}
