import os

abs_path = os.path.dirname(__file__)
MAX_SCORE = 100
MIN_SCORE = 0
INITIAL_SCORE = 10
Main_Url = "HTTP://fund.eastmoney.com/fundguzhi.html"
Basic_URL = "http://fund.eastmoney.com/"
FireFoxBinary = abs_path + "/../firefox/firefox"
Proxy_Server = "http://0.0.0.0:6101/random"
Spider_Amount = 1000
Message_Box = 100
RefreshTime = 1
Specialchars = abs_path + "/../var/specialchars"
MysqlLog = abs_path + "/../log/Jijinwang/mysql.log"
ExceptionLog = abs_path + "/../log/Jijinwang/exception.log"
ext_SQL = {"Jijinwang": "ALTER TABLE jj_info_company_admin MODIFY COLUMN `resume` text \
CHARACTER SET utf8 COLLATE utf8_general_ci NULL COMMENT '个人简历' AFTER `position`;\
ALTER TABLE jj_info_company_party MODIFY COLUMN `resume` text \
CHARACTER SET utf8 COLLATE utf8_general_ci NULL COMMENT '个人简历' AFTER `position`;\
ALTER TABLE `Jijinwang`.`jj_info_all_gonggao` \
MODIFY COLUMN `content` longtext CHARACTER SET utf8 COLLATE utf8_general_ci NULL COMMENT '内容' AFTER `date`;\
ALTER TABLE `Jijinwang`.`jj_info_manager` \
MODIFY COLUMN `introduction` longtext CHARACTER SET utf8 COLLATE utf8_general_ci NULL COMMENT '基金经理简介' AFTER `best_repay`;"}
