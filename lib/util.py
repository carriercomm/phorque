import logging
import subprocess

from ConfigParser import SafeConfigParser
from optparse import OptionParser


LOG = logging.getLogger(__name__)


class Command(object):
    def __init__(self, args=[]):
        self.stdout = None
        self.stderr = None
        self.args = args

    def execute(self, communicate=True):
        if communicate:
            process = subprocess.Popen(self.args, shell=True, 
                                       executable="/bin/bash",
                                       stdout=subprocess.PIPE, 
                                       stderr=subprocess.PIPE)
            (self.stdout, self.stderr) = process.communicate()
            return process.returncode
        else:
            pid = subprocess.Popen(self.args, shell=True, 
                                   executable="/bin/bash").pid
            return pid


def read_config(config_file):
    config = SafeConfigParser()
    config.read(config_file)
    return config

def parse_options():
    parser = OptionParser()
    parser.add_option("-c", "--config_file", action="store", dest="config_file",
                      help="Location of the config file.")
    parser.add_option("-d", "--debug", action="store_true", dest="debug",
                      help="Enable debugging log level.")
    parser.set_defaults(config_file="etc/phorque.conf")
    parser.set_defaults(debug=False)
    (options, args) = parser.parse_args()
    return (options, args)
    
def read_file(filename):
    with open(filename, "r") as f:
        return f.read()
    
def write_file(filename, contents):
    with open(filename, "w") as f:
        f.write(contents)