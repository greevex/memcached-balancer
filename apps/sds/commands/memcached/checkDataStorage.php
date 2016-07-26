<?php

namespace mpcmf\apps\sds\commands\memcached;

use mpcmf\system\application\consoleCommandBase;
use mpcmf\system\threads\threadPool;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Check storage of data on memcached
 *
 * @author Gregory Ostrovsky <greevex@gmail.com>
 */
class checkDataStorage
    extends consoleCommandBase
{
    /**
     * Define arguments
     *
     * @return mixed
     */
    protected function defineArguments()
    {
        $this->addOption('memcached', null, InputOption::VALUE_REQUIRED, 'Memcached server to check', '127.0.0.1:11211');
        $this->addOption('count', null, InputOption::VALUE_REQUIRED, 'Requests count for every check', 1000);
    }

    private $memcachedServer = '127.0.0.1:11211';
    private $requestsCount = 1000;

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

        $memcached = new \Memcached();
        $memcached->addServer($this->memcachedServer['host'], $this->memcachedServer['port']);

        $keyPrefix = 'someKey:';

        for($i = $this->requestsCount; $i > 0; $i--) {
            $memcached->set("{$keyPrefix}{$i}", $i+1000);
        }

        $ok = 0;
        for($i = $this->requestsCount; $i > 0; $i--) {
            $result = $memcached->get("{$keyPrefix}{$i}");
            $expectedResult = $i+1000;
            if($result !== $expectedResult) {
                error_log("Invalid stored result: {$result} != {$expectedResult}");
            } else {
                $ok++;
            }
        }

        $output->writeln("Ok: {$ok} / Total: {$this->requestsCount}");
    }
}
