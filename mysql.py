#!/usr/bin/env python 
# -*- coding:utf-8 -*-
import pymysql
from elasticsearch import Elasticsearch

# 打开数据库连接（ip/数据库用户名/登录密码/数据库名）
db = pymysql.connect("10.46.181.38", "root", "cdata.23", "financial",charset='utf8')
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()
sql = "SELECT * FROM financial_payment_analysis_total"
es = Elasticsearch('192.168.79.129:9200')
try:

    # 使用 execute()  方法执行 SQL 查询
    cursor.execute(sql)
    # 获取所有记录列表
    results = cursor.fetchall()
    new = []
    i = 0
    for row in results:

           i=i+1
           es.index(index='financial', doc_type='financial_payment_analysis_total', body={
              'id': i,
              'ID': row[0],
              'payment_account_name':row[2],
              'total_transaction_amount':row[9]
        })
except:
    print("Error: unable to fecth data")
# 关闭数据库连接
db.close()

