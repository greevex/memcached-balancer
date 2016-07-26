<?php

namespace mpcmf\apps\sds\libraries;

use React\EventLoop\LoopInterface;

/**
 * Class mcRouter
 *
 * @package mpcmf\apps\sds\libraries
 * @author greevex
 * @date   : 7/26/16 11:07 AM
 */
class mcRouter
{
    /** @var LoopInterface  */
    private $loop;

    /** @var array  */
    private $config;

    /**
     * mcRouter constructor.
     *
     * @param LoopInterface $loop
     * @param array         $config
     */
    public function __construct(LoopInterface $loop, array $config)
    {
        $this->loop = $loop;
        $this->config = $config;
    }

    public function register()
    {
        //@todo bind and listen
    }
}