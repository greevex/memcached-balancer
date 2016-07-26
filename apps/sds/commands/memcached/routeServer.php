<?php

namespace mpcmf\apps\sds\commands\memcached;

use mpcmf\system\application\consoleCommandBase;
use mpcmf\system\threads\thread;
use React\Dns\Resolver\Factory as reactResolver;
use React\EventLoop\Factory;
use React\Socket\Connection;
use React\Socket\Server as reactSocketServer;
use React\SocketClient\Connector;
use React\Stream\Stream as reactStream;
use React\Stream\Stream;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Async memcached routing server
 *
 * @author Gregory Ostrovsky <greevex@gmail.com>
 */
class routeServer
    extends consoleCommandBase
{
    private $childPorts = [];

    /** @var thread[] */
    private $threads = [];

    private $childHost;
    private $port;

    /** @var OutputInterface */
    private $output;

    /** @var  Stream */
    private $roMemcachedStream;

    /** @var  Stream */
    private $rwMemcachedStream;

    /**
     * Define arguments
     *
     * @return mixed
     */
    protected function defineArguments()
    {
        $this->addOption('bind', 'b', InputOption::VALUE_REQUIRED, 'Host to bind', '127.0.0.1');
        $this->addOption('ports', 'p', InputOption::VALUE_REQUIRED, 'Ports');
        $this->addOption('master-server', 'm', InputOption::VALUE_REQUIRED, 'Start master server on this host:port');

        $this->addOption('old-mc-server', null, InputOption::VALUE_REQUIRED, 'Old memcached server to drain from');
        $this->addOption('new-mc-server', null, InputOption::VALUE_REQUIRED, 'New memcached server to drain to');
    }

    private $oldServer;
    private $newServer;

    /**
     * Executes the current command.
     *
     * This method is not because you can use this class
     * as a concrete class. In this case, instead of defining the
     * execute() method, you set the code to execute by passing
     * a Closure to the setCode() method.
     *
     * @param InputInterface  $input  An InputInterface instance
     * @param OutputInterface $output An OutputInterface instance
     *
     * @return null|int null or 0 if everything went fine, or an error code
     *
     * @throws \LogicException When this method is not implemented
     *
     * @see setCode()
     */
    protected function handle(InputInterface $input, OutputInterface $output)
    {
        $this->output = $output;
        $this->prepareThreads($input);
        $this->checkThreads();

        $this->oldServer = $this->parseAddr($input->getOption('old-mc-server'));
        $this->newServer = $this->parseAddr($input->getOption('new-mc-server'));

        $masterServerAddr = $input->getOption('master-server');
        if($masterServerAddr) {
            $this->masterServer($this->parseAddr($masterServerAddr));
        } else {
            for(;;) {
                $this->checkThreads();
                sleep(1);
            }
        }
    }

    public function checkThreads()
    {
        foreach($this->threads as $addr => $thread) {
            if(!$thread->isAlive()) {
                MPCMF_DEBUG && $this->output->writeln("<error>Starting server on {$addr}</error>");
                try {
                    $thread->start($addr);
                } catch(\Exception $e) {
                    error_log("Unable to start server, cuz exception: {$e->getMessage()}\n{$e->getTraceAsString()}");
                }
                usleep(250000);
            }
        }
    }

    protected function prepareThreads(InputInterface $input)
    {

        $this->childHost = $input->getOption('bind');
        $portsString = $input->getOption('ports');
        if(empty($this->childHost) || empty($portsString)) {
            error_log('--bind & --ports required params');
            exit;
        }
        if(empty($portsString)) {
            $portsString = file_get_contents(APP_ROOT . '/.prefork_config');
        }
        $this->childPorts = [];
        foreach(explode(',', $portsString) as $port) {
            $port = trim($port);
            if(empty($port)) {
                continue;
            }
            $this->childPorts[$port] = true;
        }
        /** @var thread[] $threads */
        $this->threads = [];

        foreach($this->childPorts as $port => $value) {
            $this->threads[json_encode($this->parseAddr("{$this->childHost}:{$port}"))] = new thread([$this, 'childServer']);
        }
    }

    protected function parseAddr($addr)
    {
        $explodedAddr = explode(':', $addr);

        return [
            'port' => array_pop($explodedAddr),
            'host' => trim(implode(':', $explodedAddr), '[]'),
        ];
    }

    private function getDns($loop)
    {
        $dnsResolverFactory = new reactResolver();
        return $dnsResolverFactory->createCached('8.8.8.8', $loop);
    }

    public function masterServer($bindMasterTo)
    {
        $output = $this->output;

        MPCMF_DEBUG && $output->writeln('<error>[MASTER]</error> Preparing server');

        $loop = Factory::create();


        $connector = new Connector($loop, $this->getDns($loop));

        $output->writeln('<error>[MASTER]</error> Binding callables and building socketServer');

        $socketServer = new reactSocketServer($loop);

        $clientId = null;

        $socketServer->on('connection', function (Connection $clientConnection) use ($connector, $output, $clientId, $loop) {

            $clientConnection->pause();

            MPCMF_DEBUG && $clientId = spl_object_hash($clientConnection);
            do {
                $threadKey = array_rand($this->threads);
                if($this->threads[$threadKey]->isAlive()) {
                    break;
                }
                $loop->tick();
            } while(true);
            $childPort = json_decode($threadKey, true)['port'];

            MPCMF_LL_DEBUG && $output->writeln("<error>[MASTER:{$clientId}]</error> Client connected, using port {$childPort}");


            $clientConnection->on('end', function() use ($clientConnection, $clientId, $output) {
                MPCMF_LL_DEBUG && $output->writeln("<error>[MASTER:{$clientId}]</error> Client connection ending");
            });
            $clientConnection->on('close', function() use ($clientConnection, $clientId, $output) {
                MPCMF_LL_DEBUG && $output->writeln("<error>[MASTER:{$clientId}]</error> Client connection closed");
            });

            /** @var \React\Promise\FulfilledPromise|\React\Promise\Promise|\React\Promise\RejectedPromise $childConnection */
            $childConnection = $connector->create($this->childHost, $childPort);
            $childConnection->then(function (reactStream $childStream) use ($clientConnection, $childConnection, $output, $clientId) {

                $childStream->pause();

                MPCMF_LL_DEBUG && $output->writeln('<error>=================== ' . spl_object_hash($childStream) . ' CHILD STREAM OPEN </error>');

                $childStream->on('end', function() use ($clientConnection, $childConnection, $childStream, $output, $clientId) {
                    MPCMF_LL_DEBUG && $output->writeln("<error>[MASTER:{$clientId}]</error> Child closed connection");
                    MPCMF_LL_DEBUG && $output->writeln('<error>=================== ' . spl_object_hash($childStream) . ' CHILD STREAM CLOSE</error>');
                    $childStream->close();

                    $clientConnection->getBuffer()->on('full-drain', function() use ($clientConnection, $output, $clientId) {
                        MPCMF_LL_DEBUG && $output->writeln("<error>[MASTER:{$clientId}]</error> Buffer is empty, closing client connection");
                        $clientConnection->close();
                    });
                });

                $childStream->on('data', function($data) use ($clientConnection, $childConnection, $childStream, $output, $clientId) {
                    MPCMF_LL_DEBUG && $output->writeln("<error>[MASTER:{$clientId}]</error> Response from child received, sending response to client");

                    $clientConnection->write($data);
                });

                $childStream->resume();

                $clientConnection->on('data', function ($data) use ($clientConnection, $childConnection, $output, $clientId, $childStream) {
                    MPCMF_LL_DEBUG && $output->writeln("<error>[MASTER:{$clientId}]</error> Client data received, sending request to child");

                    $childStream->write($data);
                });

                $clientConnection->resume();
            });
        });



        $output->writeln("<error>[MASTER]</error> Starting server on {$bindMasterTo['host']}:{$bindMasterTo['port']}");
        $socketServer->listen($bindMasterTo['port'], $bindMasterTo['host']);

        $loop->addPeriodicTimer(1.0, [$this, 'checkThreads']);
        $loop->run();
    }

    public function childServer($addr)
    {
        $bindTo = json_decode($addr, true);
        $this->childHost = $bindTo['host'];
        $this->port = $bindTo['port'];

        cli_set_process_title('mpcmf routeServer-child');

        posix_setgid(99);
        posix_setuid(99);
        posix_seteuid(99);
        posix_setegid(99);

        $loop = Factory::create();

        $connector = new Connector($loop, $this->getDns($loop));

        $socket = new reactSocketServer($loop);

        $socket->on('connection', function (Stream $clientStream) use ($connector) {
            if(!$this->roMemcachedStream || !$this->rwMemcachedStream) {
                $clientStream->pause();
            }

            if(!$this->roMemcachedStream) {
                /** @var \React\Promise\FulfilledPromise|\React\Promise\Promise|\React\Promise\RejectedPromise $connection */
                $connection = $connector->create($this->oldServer['host'], $this->oldServer['port']);
                $connection->then(function (Stream $memcachedStream) use ($clientStream) {
                    $this->roMemcachedStream = $memcachedStream;
                    if($this->rwMemcachedStream) {
                        $clientStream->resume();
                    }
                });
            }

            if(!$this->rwMemcachedStream) {
                /** @var \React\Promise\FulfilledPromise|\React\Promise\Promise|\React\Promise\RejectedPromise $connection */
                $connection = $connector->create($this->newServer['host'], $this->newServer['port']);
                $connection->then(function (Stream $memcachedStream) use ($clientStream) {
                    $this->rwMemcachedStream = $memcachedStream;
                    if($this->roMemcachedStream) {
                        $clientStream->resume();
                    }
                });
            }

            $clientStream->on('data', function($clientRequestData) use ($clientStream) {
                if(strpos($clientRequestData, 'get') === 0) {
                    // RO-server (old)
                    $this->roMemcachedStream->once('data', function($data) use ($clientStream, $clientRequestData) {
                        if(strpos($data, 'END') === 0 && strlen($data) === 5) {
                            $this->rwMemcachedStream->once('data', function($data) use ($clientStream) {
                                $clientStream->write($data);
                            });
                            $this->rwMemcachedStream->write($clientRequestData);
                        } else {
                            $clientStream->write($data);
                        }
                    });
                    $this->roMemcachedStream->write($clientRequestData);
                } else {
                    // RW-server (new)
                    $this->rwMemcachedStream->once('data', function($data) use ($clientStream) {
                        $clientStream->write($data);
                    });
                    $this->rwMemcachedStream->write($clientRequestData);
                }
            });
        });

        $this->output->writeln("<error>[CHILD]</error> Starting child server on {$this->childHost}:{$this->port}");
        $socket->listen($this->port, $this->childHost);
        $loop->run();
    }
}
