from os import environ
import asyncio
from autobahn.asyncio.wamp import ApplicationSession, ApplicationRunner
from autobahn.wamp.serializer import UBJSONSerializer
from autoreconnect import ApplicationRunner
from fileio import *

class CrossbarAppSession(ApplicationSession):
    appSession = u"Session 'mtscomm-publisher'"
    #filewatcher.start()
    filewatcher = FileWatcher()
    logger = filewatcher.logger_

    def onLeave(self, details):
        self.logger.info("Session left")

    def onDisconnect(self):
        self.logger.info("Transport disconnected")

    def onConnect(self):
        self.logger.info("{} connected".format(self.appSession))
        self.join(self.config.realm, [u'anonymous'])

    async def onJoin(self, details):
        self.logger.info('Joined to {} realm'.format(details.realm))
        while True:
            for content in self.filewatcher.processData():
                self.publish('mtscomm',content)
            await asyncio.sleep(5)


if __name__ == '__main__':
    ubjsons = UBJSONSerializer()
    crossbarHost = '10.8.1.5:9000'
    realm = 'crossbar-umetmss'
    level = 'info'
    runner = ApplicationRunner(environ.get("UMET-MSS_ROUTER", u"ws://" + crossbarHost + "/ws"), realm,
                               extra={'openHandshakeTimeout': 10,
                                      'closeHandshakeTimeout': 5,
                                      'autoPingInterval': 10,
                                      'autoPingTimeOut': 5},
                               serializers=[ubjsons]
                               )
    runner.run(CrossbarAppSession, log_level=level)