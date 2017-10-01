import re
import datetime
from collections import namedtuple

from message import *

GtsHeader_RE = re.compile(r"""(?P<gtsHeader>
                                           (?P<gtsTTAAii>[A-ZА-Я]{4}\d{2})\s
                                           (?P<gtsCCCC>[A-ZА-Я]{4})\s
                                           (?P<gtsYYGGgg>(?P<YY>\d{2})
                                           (?P<GGgg>\d{4}))
                                           (\s(?P<gtsBBB>[A-ZА-Я]{3}))?)""",
                          re.VERBOSE)

GtsSeqNumber_RE = re.compile(r"^(?P<gtsSeqNumber>\d{3}$)")

sizeBlock_RE = re.compile(r"^(?P<sizeblock>\d{10}$)")
gmcHeader_RE = re.compile(r"(?P<type>\d{3})\s(?P<address>\d{6}/)\s+?(?P<number>=\S\d{3})?(?P<GGgg>=\d{4})?\x02?")
sadisHeader_RE = re.compile(r"(?P<sizeBlock>\d{10})\x01\s+(?P<seqNumber>\d{3})\s+")
wmoHeader_RE = re.compile(r"\x01\s+(?P<seqNumber>\d{3})\s+")

BULLETIN_ALL_TYPES = {
    "AB Weather summaries",
    "AC Convective outlooks",
    "AS Surface analyses",
    "AU Upper level analyses",
    "AX Tropical discussions",
    "CS Climatic data",
    "CU Upper air climatic data",
    "FA Area forecasts",
    "FB Aviation forecasts",
    "FC Recovery forecasts",
    "ФЦ Recovery forecasts",
    "FD Winds aloft forecasts",
    "FE Extended forecasts",
    "FK Air stagnation forecasts",
    "FO Model output forecasts",
    "FP Public forecasts",
    "FQ Metropolitan forecasts",
    "FS Surface forecasts",
    "FT Terminal forecasts",
    "ФТ Terminal forecasts",
    "FU Upper level forecasts",
    "FV Avalanche forecasts",
    "FW Recreational forecasts",
    "FX Prog discussions",
    "FZ Marine forecasts",
    "NF Special notices",
    "NO General notices",
    "RW River conditions, flood info and forecasts",
    "SA Surface observations",
    "СА Surface observations",
    "яю Surface observations",
    "SD Radar observations",
    "SE Earthquake observations",
    "SF Sferics weather data",
    "SH Synoptic ship reports",
    "SI Intermediate synoptic reports",
    "SM Synoptic observations",
    "SP Special reports",
    "СП Special reports",
    "SR River and rainfall observations",
    "SS Ship reports",
    "ST Ice reports",
    "SX Miscellaneous observations",
    "TB Satellite data",
    "UA Pilot reports, Airep",
    "UC Upper air data from ships",
    "UE Upper air data from ships",
    "UF Upper air data from ships",
    "UG Pibal/Rawinsonde data",
    "UH Pibal/Rawinsonde data",
    "UI Pibal/Rawinsonde data",
    "UJ Radiosonde data",
    "UK Radiosonde data",
    "UM Radiosonde data",
    "UN Radiosonde data",
    "UQ Radiosonde data",
    "UP Pibal/Rawinsonde data",
    "UR Aircraft reconnaissance data",
    "US Radiosonde data",
    "UW Radiosonde data",
    "UX Radiosonde data",
    "UT Aircraft reports",
    "UY Upper air data",
    "UZ Upper air data",
    "WA Airmet",
    "WF Tornado warnings",
    "WO WAREP",
    "WR Flash flood warnings",
    "WS Sigmets",
    "WC Sigmets",
    "WV Sigmets",
    "WT Tropical storm/hurricane advisories",
    "WU Severe thunderstorm warnings",
    "WW WAREP"
}

BULLETIN_TYPES = {
    "FC Recovery forecasts",
    "ФЦ Recovery forecasts",
    "FT Terminal forecasts",
    "ФТ Terminal forecasts",
    "SA Surface observations",
    "СА Surface observations",
    "яю Surface observations",
    "SP Special reports",
    "СП Special reports",
    "WS Sigmets",
    "WC Sigmets",
    "WV Sigmets",
    "WA Airmet",
    "FA Area forecasts, GAMET",
    "UA Pilot reports, Airep",
    "WO WAREP",
    "WW WAREP"
}

handlerMessage = namedtuple('Handler', 'process msg')
messageHandler = namedtuple('msgHandler', 'process')
'''
BULLETIN_TYPES = {
    "SA": handlerMessage(handler=Message, msg = ""),
    "СА": handlerMessage(handler=Message, msg = ""),
    "яю": handlerMessage(handler=Message, msg = ""),
    "SP": handlerMessage(handler=Message, msg = ""),
    "СП": handlerMessage(handler=Message, msg = "")
}
'''

BULLETIN_TYPES = {
    'SA': messageHandler(process=Metar),
    'СА': messageHandler(process=Metar),
    'яю': messageHandler(process=Metar),
    'SP': messageHandler(process=Speci),
    'СП': messageHandler(process=Speci),
    'FC': messageHandler(process=Taf),
    'FT': messageHandler(process=Taf),
    'ФТ': messageHandler(process=Taf),
    'ФЦ': messageHandler(process=Taf),
    'WC': messageHandler(process=Sigmet),
    'WS': messageHandler(process=Sigmet),
    'WV': messageHandler(process=Sigmet),
    'WA': messageHandler(process=Airmet),
    'FA': messageHandler(process=Gamet),
    'WW': messageHandler(process=Warep),
    'WO': messageHandler(process=Warep)
    #'UD': messageHandler(process=Amdar)
}

# Exceptions
class BulletinError(Exception):
    """Exception raised """
    pass

class Bulletin:
    def __init__(self, logger, data, encoding, lineSeparator='\r\r\n'): #TODO test line separator
        self.logger = logger
        self.rawData = data
        self.header = ''
        self.headerGtsTT = ''
        self.endMessage = '='
        self.dtime = ''
        self.dtEmmision = None
        self.messages = []
        self.encoding = encoding
        self.lineSeparator = lineSeparator
        self.binary = self.isBinary(data)
        self.startLine = chr(1)
        self.endLine = chr(3)
        self.seqNumber = 0
        self.handle(self.rawData)

    def isBinary(self, data):
        try:
            datastr = str(data)
            if datastr.find('BUFR', 0, 100) != -1 or datastr.find('GRIB', 0, 100) != -1 \
                    or datastr.find('FAX', 0, 100) != -1 or datastr.find('PNG', 0, 100) != -1:
                return True
            return False
        except:
            raise BulletinError

    def handleMessages(self, rawMessages):  # TODO refactoring

        rawmessages = rawMessages[:]
        result = []
        while True:
            try:
                line = ' '.join(rawmessages.pop(0).split())
                if line.endswith(self.endMessage):
                    result.append(line)
                elif line != 'NIL':
                    tmpline = [line, ]
                    while True:
                        tmpline.append(' '.join(rawmessages.pop(0).split()))
                        if tmpline[-1].endswith(self.endMessage):
                            tmpline = ' '.join(tmpline)
                            break
                    result.append(tmpline)
            except IndexError:
                break
        return result

    def handle(self, data):
        self.blocks = self.handleBlocks(data)

        if self.blocks:
            for block in self.blocks:
                block = block.split(self.lineSeparator)
                messages = []
                for idx, line in enumerate(block):  # TODO need to refactoring
                    try:
                        self.header = GtsHeader_RE.search(line).groupdict()
                        self.logger.info('{}'.format(self.header['gtsHeader']))
                        self.headerGtsTT = self.header['gtsTTAAii'][:2]
                        self.dtime = self.header['gtsYYGGgg']
                        self.dtEmmision = self.computeEmissionDt()
                    except AttributeError:
                        continue
                    messages = block[idx + 1:]
                    break
                else:
                    self.logger.error("Can't find WMO header in bulletin {}".format(block[0]))  # TODO need to analyse
                    return
                if messages:
                    self.messages = self.handleMessages(messages)
                    try:
                        mssgHandler = BULLETIN_TYPES[self.headerGtsTT]
                    except:
                        self.logger.debug('No handler for process type of bulletin [{}]'.format(self.headerGtsTT))
                    else:
                        for message in self.messages:
                            mssgHandler.process(message, self.logger, self.dtEmmision)

                else:
                    self.logger.error('Messages block in bulletin {} is empty'.format(self.header['gtsHeader']))

    def handleBlocks(self, data):

        maskEnd = chr(3)
        def detectHeader(str):
            header = ''
            if gmcHeader_RE.search(str):
                header = gmcHeader_RE.search(str).group(0)
                if header:
                    maskBeg = header
                    return len(maskBeg)
            if sadisHeader_RE.search(str):
                header = sadisHeader_RE.search(str).group(0)
                if header:
                    maskBeg = header
                    return len(maskBeg)
            if wmoHeader_RE.search(str):
                header = wmoHeader_RE.search(str).group(0)
                if header:
                    maskBeg = header
                    return len(maskBeg)
            return 0

        bulletinBlocks = []
        if not self.binary:
            try:
                data = data.decode(self.encoding)
            except UnicodeDecodeError:
                data = data.decode('iso8859-1')
        while data:
            inxBeg = detectHeader(data[:20])
            inxEnd = data.find(maskEnd)

            if inxBeg > -1 and inxEnd > -1:
                bulletinBlocks.append(data[inxBeg:inxEnd])
                data = data.replace(data[:inxEnd+1], '')

        return bulletinBlocks

    def computeEmissionDt(self): #TODO refactoring
        try:
            dtTmp = datetime.datetime.utcnow().replace(second=0, microsecond=0)
            dtTmpNow = dtTmp
            dtime = self.dtime
            day = int(dtime[:2])
            hour = int(dtime[2:4])
            minute = int(dtime[4:])
            if day > 31:
                self.logger.error("Error in bulletin header [{}]; group [YYGGgg], day={} must be in [0..31]"
                                  .format(self.header['gtsHeader'], day))
                dtTmp = dtTmp.replace(day=dtTmpNow.day)
                self.logger.debug("Bulletin emission day was setting to now day={}".format(dtTmpNow.day))
            else:
                dtTmp = dtTmp.replace(day=day)
            if hour >= 24:
                self.logger.error("Error in bulletin header [{}]; group [YYGGgg], hour={} must be in [0..24]"
                                  .format(self.header['gtsHeader'], hour))
                dtTmp = dtTmp.replace(hour=dtTmpNow.hour)
                self.logger.debug("Bulletin emission hour was setting to now, hour={}".format(dtTmpNow.day))
            else:
                dtTmp = dtTmp.replace(hour=hour)
            if minute >= 60:
                self.logger.error("Error in bulletin header [{}]; group [YYGGgg], minute={} must be in [0..60]"
                                  .format(self.header['gtsHeader'], minute))
                dtTmp = dtTmp.replace(minute=dtTmpNow.minute)
                self.logger.debug("Bulletin emission minute was setting to now, minute={}".format(dtTmpNow.minute))
            else:
                dtTmp = dtTmp.replace(minute=minute)
        except:
            self.logger.error("Error in bulletin header [{}]. Can't analyse emission date and time in group [YYGGgg]={}"
                              .format(self.header['gtsHeader'], self.dtime))
            self.logger.debug("Bulletin emission date and time of [{}] was setting to now: {}"
                              .format(self.header['gtsHeader'],dtTmpNow))
            return dtTmpNow
        return dtTmp

    def getHeader(self, data):
        try:
            gtsheader = GTSHeader_RE.search(str(data)).groupdict()
            self.logger.info('{}'.format(gtsheader['gtsHeader']))
            return gtsheader['gtsHeader']
        except AttributeError:
            gtsheader = 'No GTS Header'
            self.logger.info('{}'.format(gtsheader))
            return gtsheader
