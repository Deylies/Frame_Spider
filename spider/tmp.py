from selenium import webdriver
from pyvirtualdisplay import Display
from bs4 import BeautifulSoup
import time

display = Display(visible=99, size=(800, 800))
display.start()
brower = webdriver.Firefox(firefox_binary="../firefox/firefox")
brower.get("http://fund.eastmoney.com/f10/jjjz_161720.html")
botton = brower.find_element_by_xpath("//div[@id='pagebar']//label[last()]")
botton.click()
time.sleep(10)
brower.close()
display.stop()
