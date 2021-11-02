<?php
namespace IncrementalTable;

use Psr\Log\LoggerInterface;


/**
 * Created by .
 * User: dredger
 * Date: 12/4/2018
 * Time: 20:01
 */
class DbTools
{

    /**
     * @var DbAdapterInterface|null
     */
    private $db = null;
    /**
     * @var null|LoggerInterface
     */
    private $logger = null;

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
     * @param int $max_queries
     * @param int $timeout
     *
     * @return bool
     */
    public function waitForLowDbLoad($max_queries = 200, $timeout = 0)
    {

        $this->logger->info("waitForLowDbLoad Start  ['max_queries'=>$max_queries, 'timeout'=>$timeout] ");
        $iteration = 0;
        while (true)
        {
            $queries_list = $this->db->mysqlFetchAllRows("SHOW PROCESSLIST");
            $queries = count($queries_list);
            $iteration++;

            if ($queries < $max_queries)
            {
                return true;
            }

            if ($timeout > 0 && $iteration > $timeout){
                return false;
            }

            $this->logger->info("waitForLowDbLoad Sleep for a second ");
            sleep(1);
        }
    }

    public function showDbLoad($where='')
    {
        $queries_list = $this->db->mysqlFetchAllRows("SHOW PROCESSLIST");
        $this->logger->info("[".count($queries_list)."] processes so far at [$where]");
    }


    /**
     * returns executuon result of SHOW PROCESSLIST
     * @return int
     */
    public function getDbLoad()
    {
        $queries_list = $this->db->mysqlFetchAllRows("SHOW PROCESSLIST");
        return count($queries_list);
    }


    /**
     *
     * @return float
     * @access public
     */
    public function getMicrotime()
    {
        list($usec, $sec) = explode(" ", microtime());
        return ((float)$usec + (float)$sec);
    }
}
