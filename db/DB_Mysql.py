import pymysql
from common.errors import MysqlClient_ValueError
from common.setting import MysqlLog
from common.config import MYSQL_conf

class Mysql_Client(object):
    def __init__(self, tablename, MYSQL):
        self.tablename = tablename
        self.db_name = MYSQL['db']
        with pymysql.connect(**MYSQL) as cursor:
            cursor.execute("SELECT `tblename` FROM target_urls")
            TABLE_LIST = [i[0] for i in cursor.fetchall()]
        if self.tablename not in TABLE_LIST:
            raise MysqlClient_ValueError
        self._db = pymysql.connect(**MYSQL)
        self._curosr = self._db.cursor()
        self.col = self.get_cols()

    def get_cols(self):
        self._curosr.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.Columns WHERE TABLE_NAME='%s' and table_schema='%s'" % (
                self.tablename, self.db_name))
        return [i[0] for i in self._curosr.fetchall()[1:]]

    def execute(self, SQL):
        try:
            self._curosr.execute(SQL)
        except pymysql.err.ProgrammingError:
            with open(MysqlLog, "a") as file:
                file.write(str(SQL) + "\n")
        self._db.commit()
        return [i[0] for i in self._curosr.fetchall()]

    def SQLInsert(self, values):
        """
        快速SQL语句，插入语句
        :param tablename: 数据库表名
        :param col: 需要插入的列名,列表
        :param values: 插入的值
        :return: 返回构造好的插入SQL语句
        """
        # if len(self.col) == len(values):
        SQL = "INSERT INTO %s (" % self.tablename + ",".join(["`%s`"] * len(self.col)) % tuple(
            self.col) + ") VALUES (" + ",".join(["'%s'"] * len(values)) % tuple(values) + ");"
        return SQL
        # else:
        #
        #     raise MysqlClient_StorageError

    def SQLUpdate(self, values, sel):
        """
        快速SQL语句，更新语句
        :param col: 需要修改的列名,列表
        :param values: 插入的值
        :param sel: 查询用的主健
        :return: 返回构造好的插入SQL语句
        """
        SQL = " UPDATE `%s` SET " % self.tablename + ",".join(
            ["`%s`='%s'" % (self.col[i], values[i]) for i in range(len(self.col))]) + " WHERE "
        where = []
        for i in range(int(len(sel) / 2)):
            where.append("`%s`='%s'" % (sel[2 * i], sel[2 * i + 1]))
        SQL += " AND ".join(where) + ";"
        return SQL

    def storage(self, values, sel):
        SQL = "SELECT * FROM %s WHERE " % self.tablename
        where = []
        for i in range(int(len(sel) / 2)):
            where.append("`%s`='%s'" % (sel[2 * i], sel[2 * i + 1]))
        SQL += " AND ".join(where) + ";"
        if len(self.execute(SQL)):
            self.execute(self.SQLUpdate(values, sel))
        else:
            self.execute(self.SQLInsert(values))

    def close(self):
        self._curosr.close()
        self._db.close()


if __name__ == "__main__":
    Mysql_Client("jj_info_property_pz",MYSQL_conf('Jijinwang')).execute("dsadsadsa")
