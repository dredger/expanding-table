<?php
/**
 * Created by .
 * User: dredger
 * Date: 12/5/2018
 * Time: 15:15
 */

use Illuminate\Foundation\Testing\WithoutMiddleware;
use Illuminate\Foundation\Testing\DatabaseMigrations;
use Illuminate\Foundation\Testing\DatabaseTransactions;


use Monolog\Logger;
use Monolog\Handler\StreamHandler;


class DbAdapterTest extends TestCase
{

    public function testLogger()
    {
        $orderLog = new Logger('test');

        $msg = "msg";
        $data = ['data'=>'value'];
        $orderLog->info($msg, $data);
    }


    public function testFetchArray(){

        $db = new \IncrementalTable\DbAdapter();

        $dbhost         = DB::connection()->getConfig('host');
        $dbusername     = DB::connection()->getConfig('username');
        $dbpasswd       = DB::connection()->getConfig('password');
        $database_name  = DB::connection()->getConfig('database');

        $db ->initConnectionMasterDb($dbhost, $dbusername, $dbpasswd, $database_name);
        $db ->initConnectionSlaveDb($dbhost, $dbusername, $dbpasswd, $database_name);

        $r = $db->mysqlFetchArray("SHOW PROCESSLIST");
        $this->assertArrayHasKey('Info', $r , "Key Info was not found.");

        $r = $db->mysqlFetchAllRows("SHOW PROCESSLIST");
        $this->assertTrue(count($r) > 0 , 'Array is empty');
    }

    public function testExec(){

        $db = new \IncrementalTable\DbAdapter();

        $dbhost         = DB::connection()->getConfig('host');
        $dbusername     = DB::connection()->getConfig('username');
        $dbpasswd       = DB::connection()->getConfig('password');
        $database_name  = DB::connection()->getConfig('database');

        $db ->initConnectionMasterDb($dbhost, $dbusername, $dbpasswd, $database_name);
        $db ->initConnectionSlaveDb($dbhost, $dbusername, $dbpasswd, $database_name);


        $r = $db->exec("SHOW PROCESSLIST", true);

        $this->assertTrue(!empty($r), 'Result is empty');

    }

}
