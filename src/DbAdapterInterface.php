<?php
namespace IncrementalTable;

/**
 * Created by .
 * User: dredger
 * Date: 12/5/2018
 * Time: 11:43
 */
interface  DbAdapterInterface
{

    /**
     * @param bool $readOnlyConnection
     * @return  \mysqli|null
     */
    public function getConnection($readOnlyConnection = false);



    /**
     * @param string $query
     * @param  bool $readOnlyConnection
     * @return bool|mysqli_result
     */
    public function mysqlQuery($query, $readOnlyConnection = false);


    /**
     * @param $query
     * @param bool $readOnlyConnection
     *
     * @return mixed
     */
    public function mysqlFetchArray($query, $readOnlyConnection = false);

    /**
     * @param string $query
     * @param bool $readOnlyConnection
     * @return mixed
     */
    public function exec($query, $readOnlyConnection = false);

    /**
     * @param $query
     * @param string $fieldName
     * @param bool $connectionSlaveDb
     * @return mixed
     */
    public function mysqlFetchField($query, $fieldName='', $connectionSlaveDb=false);

    /**
     * @param string $query
     * @param string $fieldName
     * @return array
     */
    public function fetchColumn($query, $fieldName);

}
