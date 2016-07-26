<?php

namespace mpcmf\apps\sds\libraries\reactEasyLibs;

use greevex\react\easyServer\communication\abstractCommunication;

/**
 * Class mcRouter
 *
 * @package mpcmf\apps\sds\libraries
 * @author greevex
 * @date   : 7/26/16 11:07 AM
 */
class mcRouterCommunication
    extends abstractCommunication
{

    private $memcached;

    private $waiting = false;
    private $cmd = null;

    /**
     * Prepare on new object initialization
     */
    protected function prepare()
    {
        $this->memcached = new \Memcached();
        $this->memcached->addServer('127.0.0.1', 11211);
    }

    /**
     * Process new received client command
     *
     * @param $command
     */
    protected function clientCommand($command)
    {
        MPCMF_LL_DEBUG && error_log("==\n+ waiting: " . ($this->waiting ? 'yes' : 'no'));

        if(!$this->waiting) {
            MPCMF_LL_DEBUG && error_log("+ input: {$command}");
            if(!preg_match('/(?<cmd>[a-z]+)\s/i', $command, $match)) {
                MPCMF_LL_DEBUG && error_log("[!!!] UNKNOWN COMMAND: {$command}");
                return;
            }
            $this->cmd = $match['cmd'];
            $this->processCommand();
        } else {
            $this->processPayload();
        }

    }

    private function processCommand()
    {
        MPCMF_LL_DEBUG && error_log("[{$this->cmd}] command processing...");
        switch($this->cmd) {
            case 'set':
                $this->waiting = true;
                break;
            case 'add':
                $this->waiting = true;
                break;
            case 'replace':
                $this->waiting = true;
                break;
            case 'append':
                $this->waiting = true;
                break;
            case 'incr':
                $this->waiting = false;
                $this->client->send('5');
                break;
            case 'decr':
                $this->waiting = false;
                $this->client->send('5');
                break;
            case 'prepend':
                $this->waiting = true;
                break;
            case 'cas':
                $this->waiting = false;
                $this->client->send('1');
                break;
            case 'get':
                $this->waiting = false;
                $this->client->send('END');
                break;
            case 'gets':
                $this->waiting = false;
                $this->client->send('END');
                break;
            case 'delete':
                $this->waiting = false;
                $this->client->send('DELETED');
                break;
        }
    }

    private function processPayload()
    {
        MPCMF_LL_DEBUG && error_log("[{$this->cmd}] payload processing...");
        switch($this->cmd) {
            case 'set':
                $this->waiting = false;
                $this->client->send('STORED');
                break;
            case 'add':
                $this->waiting = false;
                $this->client->send('STORED');
                break;
            case 'replace':
                $this->waiting = false;
                $this->client->send('STORED');
                break;
            case 'append':
                $this->waiting = false;
                $this->client->send('STORED');
                break;
            case 'prepend':
                $this->waiting = false;
                $this->client->send('STORED');
                break;
        }
    }
}