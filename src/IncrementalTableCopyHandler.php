<?php
namespace IncrementalTable;

/**
 * Created by .
 * User: dredger
 * Date: 12/4/2018
 * Time: 19:54
 */
class IncrementalTableCopyHandler
{
    /**
     * @var string
     */
    private $tableName='';

    /**
     * @var string
     */
    private $pkColumnName='';
    /**
     * @var int
     */
    private $startId=0;
    /**
     * @var int
     */
    private $finishId=0;
    /**
     * @var bool
     */
    private $ignorePk= false;

    /**
     * @var bool
     */
    private $slaveOly=false;

    /**
     * @var DbAdapterInterface|null
     */
    private $db = null;
    /**
     * @var null|LoggerInterface
     */
    private $logger = null;

    /**
     * @var IncrementalQuery
     */
    private $iq = null;

    /**
     * @var null
     */
    private $lastCopiedId  = null;

    /**
     * The number of lines to be used for copying one portion
     * @var null
     */
    private $chunkSize  = 0;


    /**
     *
     * max DB process amount after which the script will be  paused, in order to avoid the server critical load of DB.
     * @var null
     */
    private $maxQueries  = 200;


    /**
     *  . Seconds to wait before maxQueries will decrease
     * @var null
     */
    private $timeout  = 0;


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

        $this->iq = new IncrementalQuery($db, $logger);
    }

    /**
     * @param $tableName
     * @param $pkColumnName
     * @param $startId
     * @param $finishId
     * @param bool $ignorePk
     * @param int $chunkSize
     * @param int $maxQueries
     * @param int $timeout
     * @return bool
     */
    public function handle($tableName, $pkColumnName , $startId, $finishId,  $chunkSize= 0, $maxQueries=200, $timeout=0 , $ignorePk = false)
    {
        $this->logger->info("Console Start ");

        $this->tableName    = $tableName;
        $this->startId      = (int) $startId;
        $this->finishId     = (int) $finishId;
        $this->ignorePk     = (bool)$ignorePk;
        $this->chunkSize    = (int) $chunkSize;
        $this->pkColumnName = $pkColumnName;
        $this->maxQueries   = $maxQueries;
        $this->timeout      = $timeout;

        if(!$this->validate())
        {
            return false;
        }

        $this->initData();

        $columns = $this->iq->getColumns();

        if(!$columns)
        {
            $this->logger->error("Seems like the table [{$this->tableName}] does not exist or it has no fields.");
            $this->logger->error("Execution was stopped");
            return false;
        }

        $this->logger->info("Following columns will be copied [$columns]" );

        $this->logger->info('------------ Starting in 5 seconds ------------ ');

        sleep(5);

        $this->iq->start();
        $this->iq->increment();

        $this->logger->info("Going the copy data from table [{$this->iq->tableName}] to the table [{$this->iq->tableName}_copy] starting from ID [ {$this->iq->startId}] to ID [" . number_format($this->iq->finishId) . "] in chunks of [{$this->iq->chunkSize}]");


        $dbTools = new DbTools($this->db, $this->logger);
        $startTime = $dbTools->getMicrotime();

        if($this->iq->isFinished()){
            $this->logger->error("Seems like table already copied");
        }
        $iteration  = 1;
        $totalIterations = number_format((int)($this->iq->getFinishId() / $this->iq->chunkSize ));


        while (!$this->iq->isFinished())
        {
            $t = $dbTools->getMicrotime();

            $this->logger->info("\n \n ---  iteration  [" . number_format($iteration) . "]/[$totalIterations]     Processing [".$this->iq->getChunkString()."]");
            $iteration ++;

            $this->iq->processChunk();
            $dbTools->waitForLowDbLoad($this->maxQueries, $timeout);

            if ($this->iq->incrementDelay > 0)
            {
                $this->logger->info("sleeping for incrementDelay [{$this->iq->incrementDelay}]");
                sleep($this->iq->incrementDelay);
            }

            $this->iq->increment();
            $executionTime = $dbTools->getMicrotime() - $t;
            $this->logger->info("\n\n *** Iteration executed in [" . number_format($executionTime, 2) ."] seconds, LastCopiedId=" . number_format($this->iq->getLastCopiedId()). "\n\n ******");
        }

        $this->iq->finish();
        $tt  = $dbTools->getMicrotime() - $startTime;

//        $this->logger->info("Console  Finish, total execution time is  $tt " . date('"H:i:s"', $tt ) );
        $this->logger->info("In seconds [$tt]" );

        return true;
    }


    private function initData()
    {
        $this->iq->tableName    = $this->tableName;
        $this->iq->pkColumnName = $this->pkColumnName;

        $this->lastCopiedId = $this->iq->getLastCopiedId();

        if($this->ignorePk)
        {
            $this->iq->startId = $this->startId;
        }else{
            $this->iq->startId = $this->lastCopiedId > $this->iq->startId ? $this->lastCopiedId +1 : $this->startId;
        }

        $this->iq->finishId = $this->finishId ? $this->finishId :  $this->iq->getFinishId();
        if($this->chunkSize > 0)
        {
            $this->iq->chunkSize = $this->chunkSize;
        }


        $this->iq->setup();
    }


    private function validate(){
        if(!$this->tableName)
        {
            $this->logger->error("Table Name I empty");
            return false;
        }

        return true;
    }

}
