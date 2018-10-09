from common.setting import Message_Box, Specialchars, ExceptionLog, MysqlLog
import time

def best_length(string):
    string = str(string)
    if len(string) % 2:
        length1 = int((Message_Box - len(string)) / 2) - 1
        length2 = length1 - 1
    else:
        length1 = length2 = int((Message_Box - len(string)) / 2 - 1) - 1
    return length1, length2


def printf(*par):
    print("   TIME   :",time.ctime())
    for index in range(len(par)):
        print("MESSAGE",index+1,":",par[index])
    print("FINISH!","*"*200,"\n")


get_digit = lambda x: ''.join([i if i.isdigit() else '' for i in x])


def special_repace(string):
    res = string
    with open(Specialchars) as file:
        tar = eval(file.read())
    for i in tar:
        res = res.replace(i[0], i[1])
    return res


def remove(x, listable):
    while x in listable:
        listable.remove(x)


def logging_sql(module='init'):
    if module == "init":
        with open(MysqlLog, "w") as file:
            file.write('')


def logging_except(e='', module='r', cls_name=''):
    if module == 'r':
        with open(ExceptionLog, "a") as file:
            file.write(cls_name + " " * 5 + str(e) + "\n")
    elif module == 'init':
        with open(ExceptionLog, "w") as file:
            file.write('')


if __name__ == "__main__":
    # print(len('jj_info_coaaaampany_party writing!'))
    # printf("jj_info_company_party writing!", "dsadsqgqgqggqqggqqg", "dsavsafsafsafsfsafsa")
    logging_except("你好大的撒旦撒旦萨")
