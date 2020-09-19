__license__ = "GNU General Public License v3"
__author__ = 'Hwaipy'
__email__ = 'hwaipy@gmail.com'

from motor.motor_tornado import MotorClient
from interactionfreepy.db.Storage import Storage

class IFConfigContext:
    def __init__(self, db):
        self.db = db

    def getLoginSalt(self, id):
        return self.userManager.getSalt(id)

    # def login(self, username, password):
    #     print('login')


class IFDataContext:
    def __init__(self, db):
        self.db = db
        self.storage = Storage(db)


class MongoDBContext:
    def __init__(self, isTest=False):
        self.__IFConfigClient = MotorClient('mongodb://{username}:{password}@{address}:{port}/{database}'.format(
            username=Config['MongoDB.IFConfig'].Username.asString(),
            password=Config['MongoDB.IFConfig'].Password.asString(),
            address=Config['MongoDB'].Address.asString(),
            port=Config['MongoDB'].Port.asInt(),
            database='IFConfig'
        ))
        self.__IFConfig = self.__IFConfigClient.get_database('IFConfig')
        if isTest:
            self.__IFConfig = self.__IFConfigClient.get_database('IFConfigTest')
        self.__IFDataClient = MotorClient('mongodb://{username}:{password}@{address}:{port}/{database}'.format(
            username=Config['MongoDB.IFData'].Username.asString(),
            password=Config['MongoDB.IFData'].Password.asString(),
            address=Config['MongoDB'].Address.asString(),
            port=Config['MongoDB'].Port.asInt(),
            database='IFData'
        ))
        self.__IFData = self.__IFDataClient.get_database('IFData')
        if isTest:
            self.__IFData = self.__IFDataClient.get_database('IFDataTest')
        self.IFConfig = IFConfigContext(self.__IFConfig)
        self.IFData = IFDataContext(self.__IFData)

        # self.__IFConfig.get_collection('C').update_one({}).upserted_id


MongoDBContextTest = MongoDBContext(True)
MongoDBContext = MongoDBContext()
