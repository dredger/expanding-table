<?php
namespace IncrementalTable;

/**
 * Created by .
 * User: dredger
 * Date: 12/5/2018
 * Time: 11:43
 */
class DbAdapter implements DbAdapterInterface
{
    /**
     * @var \mysqli|null
     */
    private $connectionSlaveDb = null;
    /**
     * @var \mysqli|null
     */
    private $connectionMasterDb     = null;


    /**
     * DbAdapter constructor.
     */
    public function __construct()
    {

    }


    /**
     * @param $dbhost
     * @param $dbusername
     * @param $dbpasswd
     * @param $database_name
     *
     * @return \mysqli|null
     */
    public function initConnectionMasterDb($dbhost, $dbusername, $dbpasswd, $database_name)
    {
        mysqli_report(MYSQLI_REPORT_ERROR | MYSQLI_REPORT_STRICT); // show mysqli errors

        if($this->connectionMasterDb){
            $this->connectionMasterDb;
        }

        $this->connectionMasterDb = mysqli_connect($dbhost,$dbusername,$dbpasswd,$database_name);
        mysqli_select_db($this->connectionMasterDb,$database_name);
        return $this->connectionMasterDb;
    }

    /**
     * @param $dbhost
     * @param $dbusername
     * @param $dbpasswd
     * @param $database_name
     * @return \mysqli|null
     */
    public function initConnectionSlaveDb($dbhost, $dbusername, $dbpasswd, $database_name)
    {
        if($this->connectionSlaveDb)
        {
                return $this->connectionSlaveDb;
        }

        $this->connectionSlaveDb = mysqli_connect($dbhost,$dbusername,$dbpasswd,$database_name);
        mysqli_select_db($this->connectionSlaveDb,$database_name);
        return $this->connectionSlaveDb;
    }


    /**
     * @param bool $connectionSlaveDb
     * @return  \mysqli|null
     */
    public function getConnection($connectionSlaveDb = false){
        if($connectionSlaveDb )
        {
            return $this->connectionSlaveDb;
        }

        return $this->connectionMasterDb;

    }

    /**
     * @param $string
     * @return string
     */
    public function escapeString($string)
    {
        return  mysqli_real_escape_string(self::getConnection(), $string);
    }


    /**
     * @param string $query
     * @param  bool $connectionSlaveDb
     * @return bool|mysqli_result
     */
    public function mysqlQuery($query, $connectionSlaveDb = false)
    {
        if(!is_string($query)){
            print_r($query);
        }

        if(!$query){
            return false;
        }

        $r = mysqli_query(self::getConnection($connectionSlaveDb), $query);

        return $r;
    }

    /**
     * @param $query
     * @param bool $connectionSlaveDb
     *
     * @return array|null
     */
    public function mysqlFetchArray($query, $connectionSlaveDb = false)
    {
        $result = self::mysqlQuery($query, $connectionSlaveDb);
        if(!$result)
        {
            return false;
        }
        return mysqli_fetch_array($result);
    }

    /**
     * @param $query
     * @param bool $connectionSlaveDb
     *
     * @return array|null
     */
    public function mysqlFetchAllRows($query, $connectionSlaveDb = false)
    {
        $result = self::mysqlQuery($query, $connectionSlaveDb);
        $res = [];

        if(!$result){
            return $res ;
        }

        while ($row=mysqli_fetch_array($result))
        {
            $res[] = $row;
        }
        return $res;
    }

    /**
     * @param string $query
     * @param bool $connectionSlaveDb
     *
     * @return mysqli_result|bool
     */
    public function exec($query, $connectionSlaveDb = false){
        return self::mysqlQuery($query);
    }


    /**
     * @param string $query
     * @param string $fieldName
     * @return array
     */
    public function fetchColumn($query, $fieldName)
    {
        $res = [];
        $r = $this->mysqlFetchAllRows($query);

        if(!$r){
            return $res;
        }

        foreach($r AS $row)
        {
            if(!isset($row[$fieldName]))
            {
                return $res;
            }

            $res[] = $row[$fieldName];
        }

        return $res;

    }

    /**
     * @param $query
     * @param string $fieldName
     * @param bool $connectionSlaveDb
     * @return array|mixed|null
     */
    public function mysqlFetchField($query, $fieldName='', $connectionSlaveDb=false)
    {
        $r = $this->mysqlFetchArray($query, $connectionSlaveDb);
        if(!$r){
            return null;
        }
        if ($fieldName)
        {
            $r = $r[$fieldName];
        }else{
            $r = array_pop($r);
        }

        return $r;
    }
}
