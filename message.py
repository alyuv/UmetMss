import re

import datetime
from abc import ABC, abstractmethod
import uuid

#WAREP_RE = r'(AVIA|STORM)?(\r\r\n)?\s?(?P<type>(WAREP)?)\s+?(?P<index>\d{5})\s(?P<YYGGgg>\d{6}).+?='
#TAF = r'(?P<type>(TAF)?(:\r\r\n)?(\s?(AMD|COR))?)(\r\r\n)?\s?(TAF)?(?P<index>[A-ZА-Я]{4})(\s+?(?P<YYGGgg>\d{6})Z)?\s+?((?P<YYGG>(\d{4}/\d{4})|(\d{6})).+?|NIL)='

#METAR_RE = re.compile(r"""(?P<type>(METAR|SPECI)?(:\r\r\n)?(\s?(COR))?)(\r\r\n)?\s?(?P<index>[A-ZА-Я]{4})\s+?((?P<YYGGgg>\d{6})Z.+?|NIL)=""",re.VERBOSE)

METAR_RE = re.compile(r"""(?P<type>\b(METAR))\s?
                          (?P<modifier>(COR))?\s?
                          (?P<idstation>\b[A-ZА-Я]{4})\s+
                          ((?P<YYGGgg>\d{6})Z)
                          (?P<content>.+?|NIL)=""", re.VERBOSE)

METAR_TIME_RE = re.compile(r"""(?P<day>\d{2})
                               (?P<hour>\d{2})
                               (?P<minute>\d{2})""", re.VERBOSE)

SPECI_RE = re.compile(r"""(?P<type>\b(SPECI))\s?
                          (?P<modifier>(COR))?\s?
                          (?P<idstation>\b[A-ZА-Я]{4})\s+
                          ((?P<YYGGgg>\d{6})Z)
                          (?P<content>.+?|NIL)=""", re.VERBOSE)

TAF_RE = re.compile(r"""(?P<type>(TAF))\s?
                        (?P<modifier>(AMD|COR))?\s?
                        (?P<idstation>[A-ZА-Я]{4})\s?
                        ((?P<YYGGgg>\d{6})Z)\s?
                        (?P<validity>\d{4}/\d{4})\s?
                        (?P<content>.+?|NIL)=""", re.VERBOSE)

TAF_TIME_RE = re.compile(r"""(?P<day_report>\d{2})
                           (?P<hour_report>\d{2})
                           (?P<minute_report>\d{2})Z(\s+
                           (?P<begin_day>\d{2})
                           (?P<begin_hour>\d{2})/
                           (?P<end_day>\d{2})
                           (?P<end_hour>\d{2}))""", re.VERBOSE)

#(?P<idstation>[A-ZА-Я]{4})\s(?P<type>(SIGMET))(\s+\w+)?\s+(?P<YYGGgg>(VALID\s+\d{6}/\d{6}))((.+|\W)*)?
#(?P<idstation>[A-ZА-Я]{4})\s(?P<type>(SIGMET))\s+(?P<seqnum>\w+)?\s+(?P<validity>(VALID\s+\d{6}/\d{6}))((.+|\W)*)?

SIGMET_RE = re.compile(r"""(?P<idstation>[A-ZА-Я]{4})\s
                           (?P<type>(SIGMET)(\s+\w+)?)\s+
                           (?P<valid>VALID)\s+
                           (?P<YYGGgg>\d{6}/\d{6})
                           ((.+|\W)*)?""", re.VERBOSE)

SIGMET_TIME_RE = re.compile(r"""VALID\s+
                                (?P<begin_day>\d{2})
                                (?P<begin_hour>\d{2})
                                (?P<begin_minute>\d{2})/
                                (?P<end_day>\d{2})
                                (?P<end_hour>\d{2})
                                (?P<end_minute>\d{2})""", re.VERBOSE)

'''
AIRMET_RE = re.compile(r"""(?P<idstation>[A-ZА-Я]{4})\s
                           (?P<type>(AIRMET))
                           (\s+\w+)?\s+
                           (?P<YYGGgg>(VALID\s+\d{6}/\d{6}))
                           ((.+|\W)*)?""", re.VERBOSE)
'''
AIRMET_RE = re.compile(r"""(?P<idstation>[A-ZА-Я]{4})\s
                           (?P<type>(AIRMET)(\s+\w+)?)\s+
                           (?P<valid>VALID)\s+
                           (?P<YYGGgg>\d{6}/\d{6})
                           ((.+|\W)*)?""", re.VERBOSE)

'''
GAMET_RE = re.compile(r"""(?P<idstation>[A-ZА-Я]{4})\s
                          (?P<type>(GAMET))
                          (\s+\w+)?\s+
                          (?P<YYGGgg>(VALID\s+\d{6}/\d{6}))
                          ((.+|\W)*)?""", re.VERBOSE)
'''
GAMET_RE = re.compile(r"""(?P<idstation>[A-ZА-Я]{4})\s
                           (?P<type>(GAMET)(\s+\w+)?)\s+
                           (?P<valid>VALID)\s+
                           (?P<YYGGgg>\d{6}/\d{6})
                           ((.+|\W)*)?""", re.VERBOSE)

AIREP_RE = re.compile(r"""(?P<type>(ARS))(\s+\w+|-)?""", re.VERBOSE)

'''
WAREP_RE = re.compile(r"""(?P<type>(AVIA|STORM))\sWAREP\s
                          (?P<idstation>\d{5})\s(?P<YYGGgg>\d{6})(0|1)\s
                          ((.+|\W)*)?=""", re.VERBOSE)
'''
WAREP_RE = re.compile(r"""(?P<type>(AVIA|STORM))\sWAREP\s
                          (?P<idstation>\d{5})\s(?P<YYGGgg>\d{6})1\s
                          (?P<body>.+?)=""", re.VERBOSE)

WAREP_TIME_RE = re.compile(r"""(?P<day>\d{2})
                               (?P<hour>\d{2})
                               (?P<minute>\d{2})""", re.VERBOSE)

AMDAR_RE = re.compile(r"(?P<type>(AMDAR))\s(?P<YYGGgg>\d{4})1\s=", re.VERBOSE)

class MessageError(Exception):
    pass

class Message(ABC):
    MESSAGE_RE = re.compile("")

    def __init__(self, message, logger, bulletinDt=datetime.datetime.utcnow()):
        self.logger = logger
        self.idstation = ""
        self.header = ""
        self.bulletinDt = bulletinDt
        self.validBegDt = None
        self.validEndDt = None
        #self.rawCode = message
        self.parsedCode = None
        self.code = self.prepare(message)
        self.handle()
        self.hash = None

    def prepare(self , message):
        message = ' '.join(message.split())
        return message

    def getHash(self):
        tmpDt = self.validBegDt.strftime("%Y-%m-%d %H:%M:%S")
        return uuid.uuid5(uuid.NAMESPACE_DNS, '{} {}'.format(tmpDt, self.code))

    def handle(self):
        def namestr(obj, namespace=globals()):
            return [name for name in namespace if namespace[name] is obj]

        try:
            mssg = self.MESSAGE_RE.search(self.code).groupdict()
            self.parsedCode = mssg
            self.header = "{} {} {}".format(mssg['type'], mssg['idstation'], mssg['YYGGgg'])
        except AttributeError:
            self.logger.error("Message [{}] doesn't match the {} pattern".format(self.code, self.__class__.__name__))
        except KeyError as e:
            e = ' '.join(e.args)
            self.logger.error("Error dictionary key=[{}] in rexexp veraible {}".format(e, namestr(self.MESSAGE_RE)))

        else:
            self.idstation = mssg['idstation']
            self.setValidityDt()

    def fixDt(self, YYGGgg):

        def toLogger(header, YYGGgg, error):
            inx = error.find('=')
            if inx != -1:
                tmpVal = error[:inx]
            self.logger.error("Header [{}] has error in group [YYGGgg]={}, {}".format(header, YYGGgg, error))
            self.logger.debug("Set current {} as valid for [{}],".format(tmpVal,header))

        dtTmp = datetime.datetime.utcnow().replace(second=0, microsecond=0)
        dtTmpNow = dtTmp
        errors = ('day={} is out of range for month','hour={} must be in 0..23', 'minute={} must be in 0..59')
        try:
            day = int(YYGGgg[:2])
            hour = int(YYGGgg[2:4])
            minute = int(YYGGgg[4:])
            needNextDay = False
            if day <= 0 or day > 31:
                toLogger(self.header, YYGGgg, errors[0].format(day))
                day = dtTmpNow.day
            if hour == 24:
                hour = 0
                needNextDay = True
            if hour < 0 or hour > 23:
                toLogger(self.header, YYGGgg, errors[1].format(hour))
                hour = dtTmpNow.hour
            if minute < 0 or minute > 59:
                toLogger(self.header, YYGGgg, errors[2].format(minute))
                minute = dtTmpNow.minute
            if needNextDay:
                fixedDt = dtTmp.replace(day=day, hour=hour, minute=minute) + datetime.timedelta(days=1)
            else:
                fixedDt = dtTmp.replace(day=day, hour=hour, minute=minute)
        except:
            fixedDt = dtTmp.replace(day=dtTmpNow.day, hour=dtTmpNow.hour, minute=dtTmpNow.minute)
            self.logger.debug("Unknowing error duaring fix [YYGGgg]={} group. Set current date and time for [{}]"
                              .format(YYGGgg,self.header))
            return fixedDt
        return fixedDt

    def fixErrorsDt(self, YYGGgg):
        dtTmp = datetime.datetime.utcnow().replace(second=0, microsecond=0)
        dtTmpNow = dtTmp
        day = int(YYGGgg[:2])
        hour = int(YYGGgg[2:4])
        minute = int(YYGGgg[4:])
        try:
            fixedDt = dtTmp.replace(day=day, hour=hour, minute=minute)
        except ValueError as e:
            error = ''.join(e.args)
            self.logger.error("Header [{}] has error in group [YYGGgg]={}, {}".format(self.header, YYGGgg, error))
            if error == 'day is out of range for month':
                fixedDt = dtTmp.replace(day=dtTmpNow.day)
                self.logger.debug("Set current day as valid date for [{}],".format(self.header))
            if error == 'hour must be in 0..23':
                fixedDt = dtTmp.replace(hour=dtTmpNow.hour)
                self.logger.debug("Set current hour as valid hour for [{}]".format(self.header))
            if error == 'minute must be in 0..59':
                fixedDt = dtTmp.replace(minute=dtTmpNow.minute)
                self.logger.debug("Set current minute as valid minute for [{}]".format(self.header))
            return fixedDt
        return fixedDt

    @abstractmethod
    def setValidityDt(self):
        pass

    def getMetadata(value):
        pass

    def getDtStamps(mType, value):
        pass


class Metar(Message):
    MESSAGE_RE = METAR_RE
    validInterval = datetime.timedelta(hours=1, minutes=10)

    def setValidityDt(self):
        YYGGgg = self.parsedCode['YYGGgg']
        self.validBegDt = self.fixDt(YYGGgg)
        self.validEndDt = self.validBegDt + self.validInterval


class Speci(Metar):
    MESSAGE_RE = SPECI_RE


class Taf(Message):
    MESSAGE_RE = TAF_RE
    validInterval = datetime.timedelta(hours=6)

    def setValidityDt(self):
        YYGGgg = self.parsedCode['YYGGgg']
        validity = self.parsedCode['validity']
        validBegDt = self.fixDt(YYGGgg)
        validEndDt = self.fixDt(validity[5:9] + '00')

        if validEndDt - validBegDt >= datetime.timedelta(hours=30):
            validEndDt = validBegDt + self.validInterval

        self.validBegDt = validBegDt
        self.validEndDt = validEndDt

class Sigmet(Message):
    MESSAGE_RE = SIGMET_RE

    def setValidityDt(self):
        validity = self.parsedCode['YYGGgg']
        validBegDt = self.fixDt(validity[:6])
        validEndDt = self.fixDt(validity[7:13])
        self.validBegDt = validBegDt
        self.validEndDt = validEndDt


class Airmet(Sigmet):
    MESSAGE_RE = AIRMET_RE


class Gamet(Sigmet):
    MESSAGE_RE = GAMET_RE


class Warep(Message):
    MESSAGE_RE = WAREP_RE
    validInterval = datetime.timedelta(hours=30)

    def setValidityDt(self):
        YYGGgg = self.parsedCode['YYGGgg']
        self.validBegDt = self.fixDt(YYGGgg)
        self.validEndDt = self.validBegDt + self.validInterval


class Amdar(Message):
    MESSAGE_RE = AMDAR_RE
    validInterval = datetime.timedelta(hours=6)

    def setValidityDt(self):
        YYGGgg = self.parsedCode['YYGGgg']
        self.validBegDt = self.fixDt(YYGGgg)
        self.validEndDt = self.validBegDt + self.validInterval
