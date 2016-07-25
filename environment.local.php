<?php

namespace mpcmf;

use mpcmf\system\configuration\environment;

//environment::setCurrentEnvironment(environment::ENV_DEBUG);
environment::setCurrentEnvironment(environment::ENV_PRODUCTION);

define('MPCMF_DEBUG', true);
define('MPCMF_LL_DEBUG', true);