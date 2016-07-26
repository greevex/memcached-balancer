<?php

namespace mpcmf\apps\sds\libraries;

use greevex\react\easyServer\server\easyServer;
use mpcmf\apps\sds\libraries\reactEasyLibs\mcRouterCommunication;
use mpcmf\apps\sds\libraries\reactEasyLibs\memcachedProtocol;
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
    private $config = [
        'bind' => '0.0.0.0',
        'port' => 11233,
        'servers' => [
            [
                'host' => '0.0.0.0',
                'port' => 11211,
                'mode' => 'ro'
            ],
            [
                'host' => '0.0.0.0',
                'port' => 11211,
                'mode' => 'rw'
            ],
        ]
    ];

    /**
     * mcRouter constructor.
     *
     * @param LoopInterface $loop
     * @param array         $config
     */
    public function __construct(LoopInterface $loop, array $config)
    {
        $this->loop = $loop;
        foreach($config as $key => $value) {
            if(array_key_exists($key, $this->config)) {
                $this->config[$key] = $value;
            }
        }
    }

    public function register()
    {
        $serverConfig = [
            'host' => $this->config['bind'],
            'port' => $this->config['port'],
            'protocol' => memcachedProtocol::class,
            'communication' => mcRouterCommunication::class,
            'inner' => $this->config['servers']
        ];

        $server = new easyServer($this->loop);
        $server->setConfig($serverConfig);
        $server->start();
    }
}