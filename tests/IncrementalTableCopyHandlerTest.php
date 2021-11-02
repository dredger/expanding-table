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


class IncrementalTableHandlerTest extends TestCase
{


    public function testHandler(){



        $db = new \IncrementalTable\DbAdapter();

        $dbhost         = DB::connection()->getConfig('host');
        $dbusername     = DB::connection()->getConfig('username');
        $dbpasswd       = DB::connection()->getConfig('password');
        $database_name  = DB::connection()->getConfig('database');

        $db ->initConnectionMasterDb($dbhost, $dbusername, $dbpasswd, $database_name);
        $db ->initConnectionSlaveDb($dbhost, $dbusername, $dbpasswd, $database_name);


        $logger = new Logger('test');
        $dbTools  = new \IncrementalTable\DbTools($db, $logger);

        $r = $dbTools ->getDbLoad();

        $this->assertTrue($r > 0 , 'Db process count is null');

        $r = $dbTools ->showDbLoad('test');

        $r = $dbTools ->waitForLowDbLoad(10,20);
    }


}
