import os
import sys
from daemonize import Daemonize

def main(args):
    os.execl(*args)

if __name__ == '__main__':
    pid_fpath = sys.argv[1]
    args = sys.argv[2:]
    d = Daemonize(app='vwdaemon', pid=pid_fpath, action=lambda: main(args))
    d.start()
