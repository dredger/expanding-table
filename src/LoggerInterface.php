<?php
namespace IncrementalTable;

/**
 * Created by .
 * User: dredger
 * Date: 12/5/2018
 * Time: 11:43
 */
interface  LoggerInterface
{

    /**
    * Write a string as information output.
    *
    * @param  string  $string
    * @param  null|int|string  $verbosity
    * @return void
    */
    public function info($string, $verbosity = null);


    /**
     * Write a string as error output.
     *
     * @param  string  $string
     * @param  null|int|string  $verbosity
     * @return void
     */
    public function error($string, $verbosity = null);



}
