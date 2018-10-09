#!/bin/bash

# 获取系统位数，将驱动文件植入系统 start
OS_version=`uname -m`
if [ ${OS_version#*_} = "32" ];then
echo  "32位系统"
tar -xvf driver/geckodriver-v0.19.1-linux32.tar.gz
mv geckodriver /usr/local/bin
elif [ ${OS_version#*_} = "64" ];then
echo  "64位系统"
tar -xvf driver/geckodriver-v0.19.1-linux32.tar.gz
mv  geckodriver /usr/local/bin
else
echo  "未知系统${OS_version}"
fi
# 获取系统位数，将驱动文件植入系统 end

# 安装依赖包
pip3 install -i https://pypi.tuna.tsinghua.edu.cn/simple -r requirements.txt

java -jar /selenium_rc/selenium-server-standalone-3.8.0.jar&

sudo yum install Xvfb,Xephyr
