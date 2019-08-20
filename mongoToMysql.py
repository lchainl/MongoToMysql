# -*- coding: UTF-8 -*-
import pymongo
import pymysql
import sys
import datetime
import logging
import time
import os
from config import *
from sql import *

reload(sys)
sys.setdefaultencoding('utf-8')

print sys.getdefaultencoding()
# 'ascii'

class Export():
    def __init__(self):
        self.mysql_client = pymysql.connect(host=mysql_url, user=mysql_user, password=mysql_password, charset='utf8')
        self.mysql_cursor = self.mysql_client.cursor()
        self.mongo_client = pymongo.MongoClient(host=mongo_url)
        self.db = self.mongo_client[mongo_database]
        self.db.authenticate(mongo_user, mongo_password)
        self.collist = self.db.list_collection_names()
        # 日志路径
        self.logPath = '/var/logs/python'
        self.create_database_sql = create_database_sql % mysql_database
        dateFileName = time.strftime("%Y-%m-%d", time.localtime(time.time()))
        if not os.path.exists(self.logPath + "/mongoToMysql"):
            os.makedirs(self.logPath + "/mongoToMysql")
        # 根据日期记录日志
        logging.basicConfig(
            level=logging.DEBUG,  # 定义输出到文件的log级别，大于此级别的都被输出
            format='%(asctime)s  %(filename)s : %(levelname)s  %(message)s',  # 定义输出log的格式
            datefmt='%Y-%m-%d %A %H:%M:%S',  # 时间
            filename=self.logPath + '/mongoToMysql' + '/' + dateFileName + '.log',  # log文件名
            filemode='a')  # 写入模式“w”或“a”
        logging.info(self.collist)
    def create_database(self):
        try:
            self.mysql_cursor.execute(self.create_database_sql)
        except Exception as e:
            logging.info("creating database %s errors %s" % (mysql_database, str(e)))

    def get_table_structure(self):

        collections = self.mongo_collection.find_one()
        if collections:
            field_list = list(self.mongo_collection.find_one().keys())
            field_list.remove('_id')
            field_sql = ''
            for i in field_list:
                field_sql = field_sql + "%s VARCHAR(100)," % i
            field_sql = field_sql.strip(',')
            self.create_table_sql = create_table_sql % (self.collection_name,
                                                    "id VARCHAR(100) PRIMARY KEY," + field_sql)
        # print(self.create_table_sql)

    def create_table(self):
        try:
            print (use_database % mysql_database)
            print (self.create_table_sql)
            self.mysql_cursor.execute(use_database % mysql_database)
            self.mysql_cursor.execute(self.create_table_sql)
        except Exception as e:
            logging.info('creating table %s errors %s' % (self.collection_name, str(e)))

    def export_data(self):
        # 获取系统当前日期
        today = datetime.datetime.now()
        endTime = today + datetime.timedelta(days=+1)
        endTime = datetime.datetime(endTime.year, endTime.month, endTime.day, 0, 0, 0)
        # 取指定前一天的日期
        startTime = today + datetime.timedelta(days=-1)
        startTime = datetime.datetime(startTime.year, startTime.month, startTime.day, 0, 0, 0)
        logging.info ('%s 同步时间段 %s -- %s ' % (datetime.datetime.now(),startTime,today))
        # 查询本时间段内一共有多少条数据
        collec_count = self.mongo_collection.find({"create_time":{"$gte":startTime,"$lte":endTime}}).count()
        logging.info('%s 本次同步了 %s 条数据 ' % (datetime.datetime.now(), collec_count))
        datas = self.mongo_collection.find({"create_time": {"$gte": startTime, "$lte": endTime}})

        # 记录成功条数
        successCount = 0
        # 记录失败条数
        errorsCount = 0
        for item in datas:
            field = ''
            value = ''
            article_ids = ''
            object_id = ''
            for k, v in item.items():
                if k == '_id':
                    field = field + 'id' + ','
                    object_id = str(v)
                else:
                    field = field + str(k) + ','
                if type(v) == list:

                    if v:
                        if isinstance(v, list):
                            value = value + '"' + ','.join('0') + '"' + ','
                        else:
                                value = value + '"' + ','.join(v) + '"' + ','
                    else:
                        value = value + '"' + ' ' + '"' + ','
                else:
                    value = value + '"' + (str(v).replace('"', '')) + '"' + ','
            field = field.rstrip(',')
            value = value.rstrip(',')
            try:
                querySql = 'select * from ' + self.collection_name + ' where id = %s'
                self.mysql_cursor.execute(querySql, (object_id))
                result = self.mysql_cursor.fetchall()
                if result:
                    # 库中已经存在该id,不再保存
                    print 'id:' + object_id + ' 已存在，不保存！'
                else:
                    print '--------------------------------------------------------'
                    print (insert_one_sql % (self.collection_name, field, value))
                    print '--------------------------------------------------------'
                    self.mysql_cursor.execute(insert_one_sql % (self.collection_name, field, value))
                    for ObjectId in article_ids:
                        logging.info("%s ------- %s" % (ObjectId, object_id))
                        insertSql = 'insert into article_morning_id(article_id, morning_id) values (%s,%s)'
                        self.mysql_cursor.execute(insertSql, (
                            str(ObjectId), str(object_id)))
            except Exception as e:
                errorsCount = errorsCount + 1
                logging.info('errors! %s inserting data %s ' % (str(item), str(e)))
            else:
                self.mysql_client.commit()
                successCount = successCount + 1

        logging.info ('%s 本次同步成功 %s 条，失败  %s 条' % (datetime.datetime(today.year,today.month,today.day,0,0,0),successCount, errorsCount))
    def close(self):
        self.mysql_cursor.close()
        self.mysql_client.close()
        self.mongo_client.close()

    def main(self):
        self.create_database()
        for collection_name in self.collist:
            # 日志记录所有集合名称
            logging.info(collection_name)
            # 打印所有集合名称
            print collection_name
            # 跳过不想同步的集合
            if collection_name =='system.profile':
                logging.info('跳过:' + collection_name)
                continue
            self.collection_name = collection_name
            self.mongo_collection = self.db[self.collection_name]
            self.get_table_structure()
            self.create_table()
            self.export_data()
        self.close()

if __name__ == '__main__':
    e = Export()
    e.main()
