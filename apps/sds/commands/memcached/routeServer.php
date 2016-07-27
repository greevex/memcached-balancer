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
        $this->oldServer = $this->parseAddr($input->getOption('old-mc-server'));
        $this->newServer = $this->parseAddr($input->getOption('new-mc-server'));

        $this->output = $output;
        $this->prepareThreads($input);
        $this->checkThreads();

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

    private $masterClients = 0;
    private $masterRps = 0;
    private $masterRpsTime = 0;
    private $masterRpsClients = 0;

    public function masterServer($bindMasterTo)
    {
        $output = $this->output;

        MPCMF_LL_DEBUG && $output->writeln('<error>[MASTER]</error> Preparing server');

        $loop = Factory::create();

        $dnsResolverFactory = new reactResolver();
        $dns = $dnsResolverFactory->createCached('8.8.8.8', $loop);

        $connector = new Connector($loop, $dns);

        $output->writeln('<error>[MASTER]</error> Binding callables and building socketServer');

        $socketServer = new reactSocketServer($loop);

        $clientId = null;

        $socketServer->on('connection', function (Connection $clientConnection) use ($connector, $output, $clientId, $loop) {

            $clientConnection->pause();

            $this->masterClients++;
            $this->masterRpsClients++;

            MPCMF_LL_DEBUG && $clientId = spl_object_hash($clientConnection);
            do {
                $threadKey = array_rand($this->threads);
                if($this->threads[$threadKey]->isAlive()) {
                    break;
                }
                $loop->tick();
            } while(true);
            $childPort = json_decode($threadKey, true)['port'];

            MPCMF_LL_DEBUG && $output->writeln("<error>[MASTER:{$clientId}]</error> Client connected, using port {$childPort}");

            $clientConnection->on('close', function() use ($clientConnection, $clientId, $output) {
                $this->masterClients--;
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
                    $this->masterRps++;
                    MPCMF_LL_DEBUG && $output->writeln("<error>[MASTER:{$clientId}]</error> Client data received, sending request to child");

                    $childStream->write($data);
                });

                $clientConnection->resume();
            });
        });



        $output->writeln("<error>[MASTER]</error> Starting server on {$bindMasterTo['host']}:{$bindMasterTo['port']}");
        $socketServer->listen($bindMasterTo['port'], $bindMasterTo['host']);

        $this->masterRps = 0;
        $this->masterRpsTime = microtime(true);

        $loop->addPeriodicTimer(5.0, [$this, 'writeStatus']);
        $loop->addPeriodicTimer(10.0, [$this, 'checkThreads']);
        $loop->run();
    }

    public function writeStatus()
    {
        if($this->masterRps === 0 && $this->masterRpsClients === 0) {

            return;
        }

        $rpsPeriod = microtime(true) - $this->masterRpsTime;
        $rps = round($this->masterRps / $rpsPeriod, 2);

        error_log(date('[Y-m-d H:i:s]') . "[STATS] {$rps} rps on {$this->masterRpsClients} clients");

        $this->masterRps = 0;
        $this->masterRpsTime = microtime(true);
        $this->masterRpsClients = $this->masterClients;
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

        $dnsResolverFactory = new reactResolver();
        $dns = $dnsResolverFactory->createCached('8.8.8.8', $loop);

        $connector = new Connector($loop, $dns);

        /** @var \React\Promise\FulfilledPromise|\React\Promise\Promise|\React\Promise\RejectedPromise $connection */
        $connection = $connector->create($this->oldServer['host'], $this->oldServer['port']);
        $connection->then(function (Stream $memcachedStream) {
            MPCMF_LL_DEBUG && error_log('[!!!] Connected to OLD memcached server: ' . json_encode($this->oldServer));
            $this->roMemcachedStream = $memcachedStream;
        });
        /** @var \React\Promise\FulfilledPromise|\React\Promise\Promise|\React\Promise\RejectedPromise $connection */
        $connection = $connector->create($this->newServer['host'], $this->newServer['port']);
        $connection->then(function (Stream $memcachedStream) {
            MPCMF_LL_DEBUG && error_log('[!!!] Connected to NEW memcached server: ' . json_encode($this->newServer));
            $this->rwMemcachedStream = $memcachedStream;
        });

        $socket = new reactSocketServer($loop);

        $socket->on('connection', function (Stream $clientStream) use ($loop) {
            if(!$this->roMemcachedStream || get_resource_type($this->roMemcachedStream->getBuffer()->stream) !== 'stream') {
                $this->roMemcachedStream = new Stream(stream_socket_client("tcp://{$this->oldServer['host']}:{$this->oldServer['port']}"), $loop);
                MPCMF_LL_DEBUG && error_log('[!!!] INFO: OLD server initialized, resuming client');
            }

            if(!$this->rwMemcachedStream || get_resource_type($this->rwMemcachedStream->getBuffer()->stream) !== 'stream') {
                $this->rwMemcachedStream = new Stream(stream_socket_client("tcp://{$this->newServer['host']}:{$this->newServer['port']}"), $loop);
                MPCMF_LL_DEBUG && error_log('[!!!] INFO: NEW server initialized, resuming client');
            }

            $clientStream->on('data', function($clientRequestData) use ($clientStream) {

                if($clientRequestData === "quit\r\n") {
                    MPCMF_LL_DEBUG && error_log('[CHILD] Client send quit command, disconnecting him...');
                    $clientStream->close();

                    return;
                }

                MPCMF_LL_DEBUG && error_log('[CHILD] Router: Master data received');

                if(strpos($clientRequestData, 'get') === 0) {
                    // RO-server (old)

                    MPCMF_LL_DEBUG && error_log('[CHILD] Router: Request is GET, binding on data from OLD memcached');
                    $this->roMemcachedStream->once('data', function($data) use ($clientStream, $clientRequestData) {

                        MPCMF_LL_DEBUG && error_log('[CHILD] Router: [GET] Received response from OLD memcached');

                        if(strpos($data, 'END') === 0 && strlen($data) === 5) {

                            MPCMF_LL_DEBUG && error_log('[CHILD] Router: [GET] But response is empty, binding to NEW memcached...');
                            $this->rwMemcachedStream->once('data', function($data) use ($clientStream) {
                                MPCMF_LL_DEBUG && error_log('[CHILD] Router: [GET] Received response from NEW memcached, sending to master');
                                $clientStream->write($data);
                                $clientStream->getBuffer()->handleWrite();
                            });

                            MPCMF_LL_DEBUG && error_log('[CHILD] Router: [GET] And sending response to NEW memcached');
                            $this->rwMemcachedStream->write($clientRequestData);
                        } else {

                            MPCMF_LL_DEBUG && error_log('[CHILD] Router: [GET] Response OK, sending it to master');
                            $clientStream->write($data);
                            $clientStream->getBuffer()->handleWrite();
                        }
                    });

                    MPCMF_LL_DEBUG && error_log('[CHILD] Router: [GET] Sending request to OLD memcached');
                    $this->roMemcachedStream->write($clientRequestData);
                } else {
                    // RW-server (new)

                    MPCMF_LL_DEBUG && error_log('[CHILD] Router: Request is WRITE, binding on data from NEW memcached');
                    $this->rwMemcachedStream->once('data', function($data) use ($clientStream) {

                        MPCMF_LL_DEBUG && error_log('[CHILD] Router: [WRITE] Received response from NEW memcached, sending to master');
                        $clientStream->write($data);
                        $clientStream->getBuffer()->handleWrite();
                    });

                    MPCMF_LL_DEBUG && error_log('[CHILD] Router: [WRITE] Sending request to NEW memcached');
                    if(MPCMF_LL_DEBUG) {
                        $this->rwMemcachedStream->getBuffer()->on('drain', function() {
                            error_log('[CHILD] Router: [WRITE] Buffer chunk-drain OK');
                        });
                        $this->rwMemcachedStream->getBuffer()->on('full-drain', function() {
                            error_log('[CHILD] Router: [WRITE] Buffer full-drain OK');
                        });
                    }

                    $this->rwMemcachedStream->write($clientRequestData);
                    $this->rwMemcachedStream->getBuffer()->handleWrite();
                }
            });
        });

        $this->output->writeln("<error>[CHILD]</error> Starting child server on {$this->childHost}:{$this->port}");
        $socket->listen($this->port, $this->childHost);

        if(MPCMF_LL_DEBUG) {
            $loop->addPeriodicTimer(1.0, function (){
                error_log('[' . getmypid() . '][' . date('Y-m-d H:i:s') . '][LOOP]Tick...');
            });
        }

        $loop->run();
    }
}
