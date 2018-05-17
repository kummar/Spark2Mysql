# -*- coding: utf-8 -*-
#sys.setdefaultencoding('utf-8')
from pyspark.sql import SparkSession,functions
import datetime
from pyspark.sql import Row


#get yesterday
class DataUtil(object):
    def getYesterday(self):#获取昨天的日期
        yesterday=datetime.datetime.today()- datetime.timedelta(days=1)#减去一天
        return yesterday.strftime('%Y%m%d')


#创建spark环境
warehouse_location = "/user/hive/warehouse"
spark = SparkSession\
        .builder\
        .appName("yiju")\
        .config("spark.sql.warehouse.dir",warehouse_location)\
        .enableHiveSupport()\
        .getOrCreate()


sc = spark.sparkContext

yesterday=DataUtil().getYesterday()
df=spark.table("dmp.yiju_compare_with_guest_top500").repartition(200)
df.withColumn('date',functions.lit(yesterday))\
    .write.mode("append").format("jdbc").option("url", "jdbc:mysql://172.16.103.174:3306/yj_dsp")\
    .option("driver","com.mysql.jdbc.Driver").option("dbtable", "t_ehouse_report2")\
    .option("user", "dspapp").option("password", "Ds.16Adm").option("useSSL", "false").save()

