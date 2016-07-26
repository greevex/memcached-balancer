<?php

namespace mpcmf\apps\sds\commands\memcached;

use mpcmf\system\application\consoleCommandBase;
use mpcmf\system\threads\threadPool;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Overload memcached test
 *
 * @author Gregory Ostrovsky <greevex@gmail.com>
 */
class overload
    extends consoleCommandBase
{
    /**
     * Define arguments
     *
     * @return mixed
     */
    protected function defineArguments()
    {
        $this->addOption('threads', null, InputOption::VALUE_REQUIRED, 'Threads to start', 1);
        $this->addOption('memcached', null, InputOption::VALUE_REQUIRED, 'Memcached server to check', '127.0.0.1:11211');
        $this->addOption('count', null, InputOption::VALUE_REQUIRED, 'Requests count for every check', 1000);
        $this->addOption('iterations', null, InputOption::VALUE_REQUIRED, 'Total iterations for all checks', 10);
    }

    private $memcachedServer = '127.0.0.1:11211';
    private $requestsCount = 1000;
    private $iterations = 1000;

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
        $mcStr = $input->getOption('memcached');
        if(strpos($mcStr, ':') === false) {
            $mcStr .= ':11211';
        }
        list($host, $port) = explode(':', $mcStr);
        $this->memcachedServer = [
            'host' => $host,
            'port' => $port,
        ];

        $this->requestsCount = (int)$input->getOption('count');
        $this->iterations = (int)$input->getOption('iterations');

        $tp = new threadPool();
        $tp->setMaxQueue(0);
        $tp->setMaxThreads((int)$input->getOption('threads'));

        do {
            if($tp->getPoolCount() < $tp->getMaxThreads()) {
                $tp->add([$this, 'memcachedTest']);
            }
            usleep(100000);
        } while($tp->hasAlive());
    }

    public function memcachedTest()
    {
        $memcached = new \Memcached();
        $memcached->addServer($this->memcachedServer['host'], $this->memcachedServer['port']);

        $stats = [
            'set' => [
                'offset' => 0,
                'count' => $this->requestsCount,
                'start' => null,
                'stop' => null,
                'result' => null,
            ],
            'get' => [
                'offset' => (int)($this->requestsCount / 2),
                'count' => $this->requestsCount,
                'start' => null,
                'stop' => null,
                'result' => null,
            ],
            'add' => [
                'offset' => (int)($this->requestsCount / 2),
                'count' => $this->requestsCount,
                'start' => null,
                'stop' => null,
                'result' => null,
            ],
            'increment' => [
                'offset' => 0,
                'count' => $this->requestsCount * 2,
                'start' => null,
                'stop' => null,
                'result' => null,
            ],
            'delete' => [
                'offset' => 0,
                'count' => $this->requestsCount * 2,
                'start' => null,
                'stop' => null,
                'result' => null,
            ],
        ];

        $result = [];

        for($iteration = $this->iterations; $iteration > 0; $iteration--) {
            foreach ($stats as $statKey => &$statData) {
                $someValue = $this->getValueFor($statKey);

                $statData['start'] = microtime(true);
                for ($i = $statData['offset']; $i < ($statData['offset'] + $statData['count']); $i++) {
                    $memcached->{$statKey}("some_long----Key__Name:{$i}", $someValue);
                }
                $statData['stop'] = microtime(true);

                $statData['result'] = round($statData['stop'] - $statData['start'], 4);
                $result[$statKey] = $statData['result'];
            }
            unset($statData);
        }

        error_log(json_encode($result, 448));
    }

    protected function getValueFor($command)
    {
        switch ($command) {
            case 'set':
                $someValue = str_repeat(md5(time()) . md5(microtime()), 3);
                break;
            case 'get':
                $someValue = null;
                break;
            case 'add':
                $someValue = str_repeat(md5(time()) . md5(microtime()), 3);
                break;
            case 'increment':
                $someValue = 5;
                break;
            case 'delete':
                $someValue = null;
                break;
            default:
                $someValue = null;
                break;
        }

        return $someValue;
    }

}
