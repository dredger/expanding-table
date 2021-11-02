<?php

namespace App\Console\Commands\Tools;

use Illuminate\Console\Command;
use DB;

/**
 * php artisan table-delete:handle  --tableName=table1 --pkColumn=id --startId=0 --finishId=0 --chunkSize=10000  --maxQueries=200 --timeout=5   -v
 *
 * php artisan table-copy:handle  -h
 *
 * @package App\Console\Commands
 */
class IncrementalTableDeleteCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'table-delete:handle 
                            {--tableName=   :  [String] Table name table data from which will be copied. Before start Please make sure that you have prepared a table with postfix _copy }
                            {--pkColumn=    :  [String] Table Primary key name. Is used to increment calculation}
                            {--startId=     :  [Int] Primary kay value.  The copy process will start from this value. 0 means the table will be copied from the beginning. }
                            {--finishId=    :  [Int] Primary kay value.  The copy process will stopped on this value. 0 means the table will be copied to the end. }
                            {--chunkSize=   :  [Int] By default is 1000. The number of lines to be used for copying one portion }
                            {--maxQueries=  :  [Int] By default is 200.  Max DB process amount after which the script will be  paused, in order to avoid the server critical load of DB.}
                            {--timeout=     :  [Int] By default is 5. Seconds to wait before maxQueries will decrease }
    ';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Incremental table delete';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        $this->info("Handle Delete  Start!");
//        $this->error("Ops, that should not happen.");

        $this->option('tableName');
        $this->option('pkColumn');
        $this->option('startId');
        $this->option('finishId');
        $this->option('chunkSize');
        $this->option('maxQueries');
        $this->option('timeout');


        $itc = new \IncrementalTable\IncrementalTableDeleteHandler($this->getDB(), $this);

        $r = $itc->handle(
            $this->option('tableName'),
            $this->option('pkColumn'),
            $this->option('startId'),
            $this->option('finishId'),
            $this->option('chunkSize'),
            $this->option('maxQueries'),
            $this->option('timeout')
        );

        if(!$r)
        {
            $this->error('Something wrong');
        }

        $this->info("Handle Delete Finish!");

    }

    private function getDB()
    {
        $db = new \IncrementalTable\DbAdapter();

        $dbhost         = DB::connection()->getConfig('host');
        $dbusername     = DB::connection()->getConfig('username');
        $dbpasswd       = DB::connection()->getConfig('password');
        $database_name  = DB::connection()->getConfig('database');

        $db ->initConnectionMasterDb($dbhost, $dbusername, $dbpasswd, $database_name);
        $db ->initConnectionSlaveDb($dbhost, $dbusername, $dbpasswd, $database_name);

        return $db;
    }
}
