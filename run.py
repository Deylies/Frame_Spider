from optparse import OptionParser
import os
from db.DB_init import db_initization
from functools import reduce

parser = OptionParser()
abs_path = os.path.dirname(__file__)


parser.add_option("-i", "--init", default=False, dest="init", help="Initizate your Mysql Database!")

parser.add_option("-l", "--list", default=False, dest='list', action='store_true',
                  help="List all spiders on this program!")

parser.add_option("--start", dest='start', default=False, help="List all spiders on this program!")

Spiders_Get = [[[i.split('.')[0] for i in file[2]] for file in os.walk(name)][0] for name in
               [abs_path+'/schedule', abs_path+'/project', abs_path+'/db_struc']]

spiders = list(reduce(lambda x, y: set(x) & set(y), Spiders_Get))
if "__init__" in spiders:
    spiders.remove("__init__")

if __name__ == "__main__":
    (options, args) = parser.parse_args()
    if options.list:
        print('\nAll spiders are listed as follows:')
        for index in range(len(spiders)):
            print(" ", index + 1, ". ", spiders[index])
        print("\n")
    if options.init:
        if options.init in spiders:
            db_initization(options.init)
        else:
            print("\nNo such spider:%s!\n" % options.init)
    if options.start:
        if options.start in spiders:
            exec("from schedule.%s import Schedule" % options.start)
            Schedule().start()
        else:
            print("\nNo such spider:%s!\n" % options.init)
