<?php

namespace mpcmf\apps\sds\libraries\reactEasyLibs;

use greevex\react\easyServer\protocol\abstractProtocol;

/**
 * Class mcRouter
 *
 * @package mpcmf\apps\sds\libraries
 * @author greevex
 * @date   : 7/26/16 11:07 AM
 */
class memcachedProtocol
    extends abstractProtocol
{

    /**
     * Try to parse something from buffer
     *
     * @return array List of parsed commands
     */
    protected function tryParseCommands()
    {
        $result = [];
        while(($pos = strpos($this->buffer, "\r\n")) !== false) {
            $line = substr($this->buffer, 0, $pos);
            $this->buffer = substr($this->buffer, $pos + 2);
            $result[] = $line;
        }

        return $result;
    }

    /**
     * Build bytes from command to send it
     *
     * @param mixed $command
     *
     * @return string
     */
    public function prepareCommand($command)
    {
        return "{$command}\r\n";
    }
}