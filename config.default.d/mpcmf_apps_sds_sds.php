<?php
/**
 * @author UStretS
 * @date   : 11/16/12 5:11 PM
 */

\mpcmf\system\configuration\config::setConfig(__FILE__, [
    'name' => 'sds',
    'slim' => [
        'debug' => true,
        'log.enabled' => true,
        'cookies.secret_key' => 'AIRGATES:COOKIE:VERSION:1',
    ]
]);