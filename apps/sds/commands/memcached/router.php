<?php

namespace mpcmf\apps\sds\commands\memcached;

use mpcmf\apps\sds\libraries\mcRouter;
use mpcmf\system\application\consoleCommandBase;
use React\EventLoop\Factory;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

/**
 * Async SDS Server
 *
 * @author Gregory Ostrovsky <greevex@gmail.com>
 */
class router
    extends consoleCommandBase
{
    /**
     * Define arguments
     *
     * @return mixed
     */
    protected function defineArguments()
    {
        $this->addOption('cfg-file', null, InputOption::VALUE_REQUIRED, 'Config file path', '/etc/mpcmf.mcrouter.conf');
    }

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
        $config = $this->loadConfig($input->getOption('cfg-file'));

        $loop = Factory::create();

        $mcRouter = new mcRouter($loop, $config);
        $mcRouter->register();

        $loop->run();
    }

    protected function loadConfig($cfgFile)
    {
        if(!file_exists($cfgFile)) {
            self::log()->addCritical("Unable to find config file [doesn't exists]: {$cfgFile}", [__METHOD__]);
            exit(1);
        }

        if(!is_readable($cfgFile)) {
            self::log()->addCritical("Unable to read config file [access denied]: {$cfgFile}", [__METHOD__]);
            exit(1);
        }

        $configStr = file_get_contents($cfgFile);
        $config = json_decode($configStr, true);
        if(!is_array($config)) {
            self::log()->addCritical("Unable to read config file [invalid json]: {$cfgFile}", [__METHOD__]);
            error_log($configStr);
            exit(1);
        }

        return $config;
    }

}
