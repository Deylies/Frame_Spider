import pymysql
import xlrd
from common.setting import ext_SQL
from common.config import MYSQL_conf, MYSQL_TMP
from common.until import printf

"""
   MYSQL数据库初始化脚本
"""


def db_initization(spider_name):
    # check if db exesists
    with pymysql.connect(**MYSQL_TMP) as cursor:
        cursor.execute("SHOW DATABASES;")
        DBs = [i[0] for i in cursor.fetchall()]
        if spider_name not in DBs:
            cursor.execute("CREATE DATABASE %s CHARSET='utf8';" % spider_name)
            printf("Create database:%s"%spider_name)
    # checking done

    MYSQL = MYSQL_conf(spider_name)
    with pymysql.connect(**MYSQL) as cursor:
        cursor.execute("SHOW TABLES;")
        Tables = cursor.fetchall()
        for table in Tables:
            cursor.execute("DROP TABLE %s;" % table)
            printf("DROP TABLE %s;" % table)
        printf("ALL TABLES CLEAR!")

    db_config = xlrd.open_workbook("db_struc/%s.xlsx" % spider_name)

    for sheet_name in db_config.sheet_names():
        if sheet_name == 'target_urls':
            # 建立主表
            sheet = db_config.sheet_by_name(sheet_name)

            # 新建表
            nrows = sheet.nrows
            # ncols = sheet.ncols
            headers = [i.value for i in sheet.row(0)]
            SQL = "CREATE TABLE %s (`id` INT PRIMARY KEY  AUTO_INCREMENT" % sheet_name
            for head in headers:
                SQL = SQL + ",`" + head + "` VARCHAR(255) "
            SQL += ");"
            with pymysql.connect(**MYSQL) as cursor:
                cursor.execute(SQL)
                printf("CREATE TABLE %s" % sheet_name)
            # 新建表

            INSERT_SQL = "INSERT INTO %s (" % sheet_name + ','.join(["`%s`"] * len(headers)) % tuple(
                headers) + ") VALUES("

            for i in range(1, nrows):
                # 插入数据

                values = [i.value for i in sheet.row(i)]
                SQL = INSERT_SQL + ','.join(["'%s'"] * len(values)) % tuple(values) + ");"
                with pymysql.connect(**MYSQL) as cursor:
                    cursor.execute(SQL)
        else:
            # 建立从表
            sheet = db_config.sheet_by_name(sheet_name)
            # 新建表
            nrows = sheet.nrows
            ncols = sheet.ncols
            comments = [i.value for i in sheet.col(0)[1:]]
            headers = [i.value for i in sheet.col(1)[1:]]
            SQL = "CREATE TABLE %s (`id` INT PRIMARY KEY  AUTO_INCREMENT" % sheet_name
            for i in range(nrows - 1):
                SQL = SQL + ",`" + headers[i] + "` VARCHAR(255) COMMENT '" + comments[i] + "'"
            SQL += ");"
            with pymysql.connect(**MYSQL) as cursor:
                cursor.execute(SQL)
                printf("CREATE TABLE %s" % sheet_name)
    ext = ext_SQL.get(spider_name)
    if ext:
        with pymysql.connect(**MYSQL) as cursor:
            cursor.execute(ext)
    printf('Database intiazation completed!')
