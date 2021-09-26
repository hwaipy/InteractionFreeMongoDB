__author__ = 'Hwaipy'

import unittest
import os

from tornado.ioloop import IOLoop
from interactionfreemongodb.MongoDBContext import MongoDBContext
from datetime import datetime, timedelta
from random import Random
from interactionfreemongodb.app import Config


class StorageTest(unittest.TestCase):
    mongoDBContextTest = MongoDBContext(Config('config.ini'), True, timezone='Asia/Shanghai')
    storage = mongoDBContextTest.IFData.storage
    collection = 'TestCollection'

    @classmethod
    def setUpClass(cls):
        async def drop():
            await StorageTest.storage.db['Storage_{}'.format('TestCollection')].drop()

        IOLoop.current().run_sync(drop)

    def setUp(self):
        pass

    def testDBFunctions(self):
        async def test():
            rnd = Random()
            iList = [i for i in range(0, 100)]
            rnd.shuffle(iList)
            for i in iList:
                await StorageTest.storage.append(StorageTest.collection, {'Content': i}, (datetime.fromisoformat('2020-07-01T00:00:00+08:00') + timedelta(seconds=i)).isoformat())
            exp = [{'FetchTime': (datetime.fromisoformat('2020-07-01T00:00:00+08:00') + timedelta(seconds=i)).isoformat(), 'Data': {'Content': i}} for i in range(0, 100)]
            self.assertTrue(await StorageTest.storage.latest(StorageTest.collection, 'FetchTime', '2020-07-01T00:01:00+08:00', {'FetchTime': 1, '_id': 0}) == {'FetchTime': '2020-07-01T00:01:39+08:00'})
            self.assertTrue(await StorageTest.storage.latest(StorageTest.collection, 'FetchTime', '2020-07-01T00:01:00+08:00', {'FetchTime': 1, '_id': 0, 'Data.Content': 1}) == exp[99])
            self.assertTrue(await StorageTest.storage.latest(StorageTest.collection, 'FetchTime', '2020-07-01T00:02:00+08:00', {'FetchTime': 1, '_id': 0, 'Data.Content': 1}) == None)
            self.assertTrue(await StorageTest.storage.first(StorageTest.collection, 'FetchTime', '2020-07-01T00:01:00+08:00', {'FetchTime': 1, '_id': 0, 'Data.Content': 1}) == exp[61])
            self.assertTrue(await StorageTest.storage.first(StorageTest.collection, 'FetchTime', '2020-07-01T00:01:50+08:00', {'FetchTime': 1, '_id': 0, 'Data.Content': 1}) == None)
            self.assertTrue(await StorageTest.storage.range(StorageTest.collection, '2020-07-01T00:00:50+08:00', '2020-07-01T00:00:52+08:00', 'FetchTime', {'FetchTime': 1, '_id': 0, 'Data.Content': 1}) == [exp[51]])
            self.assertTrue(await StorageTest.storage.range(StorageTest.collection, '2020-07-01T00:00:40+08:00', '2020-07-01T00:00:48.444+08:00', 'FetchTime', {'FetchTime': 1, '_id': 0, 'Data.Content': 1}) == exp[41:49])
            self.assertTrue(await StorageTest.storage.get(StorageTest.collection, '2020-07-01T00:01:20+08:00', 'FetchTime', {'FetchTime': 1, '_id': 0, 'Data.Content': 1}) == exp[80])
            self.assertTrue(await StorageTest.storage.get(StorageTest.collection, '2020-07-01T00:01:20.121+08:00', 'FetchTime', {'FetchTime': 1, '_id': 0, 'Data.Content': 1}) == None)
            await StorageTest.storage.delete(StorageTest.collection, '2020-07-01T00:00:50+08:00', 'FetchTime')
            await StorageTest.storage.delete(StorageTest.collection, '2020-07-01T00:00:50+08:00', 'FetchTime')
            self.assertTrue(await StorageTest.storage.range(StorageTest.collection, '2020-07-01T00:00:40+08:00', '2020-07-01T00:01:00+08:00', 'FetchTime', {'FetchTime': 1, '_id': 0, 'Data.Content': 1}) == exp[41:50] + exp[51:60])
            self.assertTrue(await StorageTest.storage.get(StorageTest.collection, '2020-07-01T00:00:50+08:00', 'FetchTime', {'FetchTime': 1, '_id': 0, 'Data.Content': 1}) == None)
            id = ((await StorageTest.storage.get(StorageTest.collection, '2020-07-01T00:00:00+08:00', 'FetchTime', {'FetchTime': 1, '_id': 1}))['_id'])
            await StorageTest.storage.update(StorageTest.collection, id, {'NewKey': 'NewValue'})
            self.assertTrue(await StorageTest.storage.latest(StorageTest.collection, 'FetchTime', '2020-07-01T00:01:35+08:00', {'FetchTime': 1, '_id': 0}) == {'FetchTime': '2020-07-01T00:01:39+08:00'})
            self.assertTrue(await StorageTest.storage.first(StorageTest.collection, 'FetchTime', filter={'FetchTime': 1, '_id': 0, 'Data.Content': 1}) == {'FetchTime': '2020-07-01T00:00:00+08:00', 'Data': {'Content': 0}})
            self.assertTrue(await StorageTest.storage.latest(StorageTest.collection, 'FetchTime', filter={'FetchTime': 1, '_id': 0, 'Data.Content': 1}) == {'FetchTime': '2020-07-01T00:01:39+08:00', 'Data': {'Content': 99}})
            latestBatch = await StorageTest.storage.latest(StorageTest.collection, 'FetchTime', '2020-07-01T00:01:35+08:00', {'FetchTime': 1, '_id': 0}, 10)
            self.assertTrue(latestBatch == [{'FetchTime': '2020-07-01T00:01:39+08:00'}, {'FetchTime': '2020-07-01T00:01:38+08:00'}, {'FetchTime': '2020-07-01T00:01:37+08:00'}, {'FetchTime': '2020-07-01T00:01:36+08:00'}])
            firstBatch = await StorageTest.storage.first(StorageTest.collection, 'FetchTime', '2020-07-01T00:00:35+08:00', {'FetchTime': 1, '_id': 0}, 5)
            self.assertTrue(firstBatch == [{'FetchTime': '2020-07-01T00:00:36+08:00'}, {'FetchTime': '2020-07-01T00:00:37+08:00'}, {'FetchTime': '2020-07-01T00:00:38+08:00'}, {'FetchTime': '2020-07-01T00:00:39+08:00'}, {'FetchTime': '2020-07-01T00:00:40+08:00'}])
            
        IOLoop.current().run_sync(test)

    def testIndexBuilding(self):
        async def test():
            expectedIndexes = ['RecordTime', 'FetchTime']
            ii = await StorageTest.storage.db.Storage_TestCollection.index_information()
            for k in ii:
                index = ii[k]['key'][0][0]
                if expectedIndexes.__contains__(index):
                    expectedIndexes.remove(index)
            self.assertTrue(len(expectedIndexes) == 0)

        IOLoop.current().run_sync(test)

    def tearDown(self):
        pass

    @classmethod
    def tearDownClass(cls):
        pass


if __name__ == '__main__':
    unittest.main()
