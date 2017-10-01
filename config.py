import os, sys, re
import yaml

class MssConfig:
    def __init__(self, config=None, args=None):
        self.userDataDir = 'data'
        self.userCacheDir = 'cache'
        self.userConfigDir = 'conf'
        self.userLogDir = 'log'

        self.userQueueDir = self.userCacheDir + '/queue'
        self.userScriptsDir = self.userCacheDir + '/scripts'

        try:
            os.umask(0)
        except:
            pass

        self.makeDir(self.userCacheDir)
        self.makeDir(self.userDataDir)
        self.makeDir(self.userConfigDir)
        self.makeDir(self.userLogDir)
        self.makeDir(self.userQueueDir)
        self.makeDir(self.userScriptsDir)

        self.logLevel = 'debug'
        self.setLogging()
        self.logger.debug("MssConfig __init__")

        self.configName = None
        self.userConfig = config

        if config != None:
            self.configName = re.sub(r'(\.yaml)', '', os.path.basename(config))
            ok, self.userConfig = self.configPath(self.programDir, config)
            self.logger.debug("config configName  %s " % self.configName)
            self.logger.debug("config userConfig  %s " % self.userConfig)

        self.setDefaults()

    def loadYaml(self, path):
        self.logger.debug("Load configuration from yaml file")
        if path == None: return
        try:
            with open(path, 'r') as configfile:
                self.configDict = yaml.load(configfile)
            self.setOptions()
        except:
            self.logger.error()

    def setLogging(self):
        import logging.handlers

        LOG_FORMAT = ('%(asctime)s [%(levelname)s] %(message)s')

        if not hasattr(self, 'logger'):
            logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
            self.logger = logging.getLogger()
            self.logpath = None
            self.logger.debug("Config set logging 1")
            return

        if self.logPath == self.logpath:
            self.logger.debug("Config set logging 2")
            if hasattr(self, 'debug') and self.debug: self.logger.setLevel(logging.DEBUG)
            return

        if self.logPath == None:
            self.logger.debug("Switching log to stdout")
            del self.logger
            self.setLogging()
            return

        self.logger.debug("switching to log file %s" % self.logpath)

        self.logpath = self.logPath
        self.timedRotatingFileHandler = logging.handlers.TimedRotatingFileHandler(self.logpath, when='midnight', interval=1,
                                                                 backupCount=20)
        fmt = logging.Formatter(LOG_FORMAT)
        self.timedRotatingFileHandler.setFormatter(fmt)

        del self.logger

        self.logger = logging.RootLogger(logging.WARNING)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(self.self.timedRotatingFileHandler)

        if self.logLevel=='debug':
            self.logger.setLevel(logging.DEBUG)

    def setLogging_(self, options):
        import logging.handlers
        import collections
        self.logOptions = collections.defaultdict(int) #TODO delete?
        if options:
            self.logOptions.update(options)

        self.name = 'test'
        self.logger_ = logging.getLogger()
        self.logDir = str(self.logOptions['dir']) if self.logOptions['dir'] else ''
        self.logFile = str(self.logOptions['file']) if self.logOptions['file'] else self.name
        self.logBackupCount = self.logOptions['backupCount'] if self.logOptions['backupCount'] else 10
        self.logWhen = str(self.logOptions['when']) if self.logOptions['when'] else 'd'
        self.logEncoding = str(self.logOptions['encoding']) if self.logOptions['encoding'] else 'cp1251'
        self.logInterval = self.logOptions['interval'] if self.logOptions['interval'] else 1
        self.maxBytes = self.logOptions['maxBytes'] if self.logOptions['maxBytes'] else 10240000

        self.logLevel = str(self.logOptions['level']) if self.logOptions['level'] else "info"
        if self.logLevel == "debug":
            self.logger_.setLevel(logging.DEBUG)

        self.loggerHandler = logging.handlers.TimedRotatingFileHandler(
            os.path.join(self.userLogDir, self.logFile), self.logWhen, self.logInterval, self.logBackupCount, self.logEncoding)
        self.logger_.addHandler(self.loggerHandler)
        self.loggerHandlerFormatter = logging.Formatter('%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
        self.loggerHandler.setFormatter(self.loggerHandlerFormatter)
        self.logger_.propagate = False

    def makeDir(self, dir):
        try:
            os.makedirs(dir, 0o775, True)
        except:
            pass

    def configPath(self, subdir, config):

        if config == None: return False, None

        if os.path.isfile(config):
            return True, config

        configName = re.sub(r'(\.yaml|\.py)', '', os.path.basename(config))

        configPath = self.userConfigDir + os.sep + subdir + os.sep + configName + ext

        if os.path.isfile(configPath):
            return True, configPath

        configPath = self.siteConfigDir + os.sep + subdir + os.sep + configName + ext

        if os.path.isfile(configPath):
            return True, configPath

        return False, config

    def setDefaults(self):

        self.logger.debug("Set defaults settings")
        self.description = "Description of publisher"
        self.active = False
        self.accept = []
        self.rootDir = None
        self.masks = []

        if not hasattr(self, 'logPath'):
            self.logPath = None

    def setGenerals(self):
        self.logger.debug("Set general settings")
        defconf = self.userConfigDir + os.sep + 'default.yaml'
        self.logger.debug("defconf = %s\n" % defconf)
        if os.path.isfile(defconf):
            self.loadYaml(defconf)

    def setOptions(self):
        self.setLogging_(self.configDict['logging'])

def main():
    if len(sys.argv) == 1:
        print(" None None")
        cfg = MssConfig(None,None)
    elif os.path.isfile(sys.argv[1]):
        args = None
        if len(sys.argv) > 2: args = sys.argv[2:]
        print(" Conf %s" % args)
        cfg = MssConfig(sys.argv[1], args)
    else:
        print(" None %s" % sys.argv[1:])
        cfg = MssConfig(None, sys.argv[1:])
    cfg.setDefaults()
    cfg.setLogging()
    cfg.setGenerals()
    cfg.loadYaml(cfg.user_config)
    sys.exit(0)
