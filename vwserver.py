from gevent import monkey; monkey.patch_socket()

import time
import os
import shlex
import shutil
import subprocess
import signal
import socket

from funcserver import RPCServer

VWOPTIONS = {
    'passes': 3,
    'bit_precision': 27,
}

def is_process_running(process_id):
    try:
        os.kill(process_id, 0)
        return True
    except OSError:
        return False

def get_free_port():
    '''
    Finds an unused TCP port
    '''
    # from http://code.activestate.com/recipes/531822-pick-unused-port/
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    addr, port = s.getsockname()
    s.close()
    return port

def sleep_until(fn):
    '''
    Sleeps until fn returns True. Performing sleeping
    in incrementing sections based on exponential
    backoff
    '''
    for t in (.1, .2, .4, .8, 1.6, 3.2, 6.4, 12.8):
        if not fn(): time.sleep(t)

    return fn()

class VW(object):
    NUM_CHILD_PROCESSES = 8

    def __init__(self, name, data_dir, log, options=None):
        self.log = log
        self.name = name
        self.data_dir = data_dir
        self.port = 0

        self.options_fpath = os.path.join(data_dir, 'options')
        self.model_fpath = os.path.join(data_dir, 'model')
        self.pid_fpath = os.path.join(data_dir, 'pid')
        self.cache_fpath = os.path.join(data_dir, 'cache')
        self.dummy_input_fpath = os.path.join(data_dir, 'dummy')

        self.options = options or self.load_options()
        self.last_used = time.time()

        self.kill_vw_processes()
        self.load_vw()

    @classmethod
    def load(cls, name, data_dir):
        return cls(name, data_dir)

    @classmethod
    def exists(cls, name, data_dir):
        return os.path.exists(data_dir)

    def kill_vw_processes(self):
        if os.path.exists(self.pid_fpath):
            pid = int(open(self.pid_fpath).read())
            try:
                os.killpg(pid, signal.SIGKILL)
            except OSError:
                pass
            os.remove(self.pid_fpath)
            return pid

    def load_vw(self):
        # ensure cache file
        open(self.cache_fpath, 'a+').close()

        # prepare dummy input (required to force vw to use empty cache
        # to start with)
        open(self.dummy_input_fpath, 'w').close()

        self.port = get_free_port()

        # user-specifiable options
        opts = [('--%s %s' % (k, v)) for k, v in self.options.iteritems()]

        # model file option
        if os.path.exists(self.model_fpath):
            opts.append('--initial_regressor %s' % self.model_fpath)
        else:
            opts.append('--final_regressor %s' % self.model_fpath)

        # standard options
        opts.extend(['--daemon', '--no_stdin', '--save_resume', '--quiet',
                    '--num_children %s' % self.NUM_CHILD_PROCESSES,
                    '--pid_file %s' % self.pid_fpath,
                    '--cache_file %s' % self.cache_fpath,
                    '--port %s' % self.port])

        # construct vw command
        cmd = 'vw %s %s' % (' '.join(opts), self.dummy_input_fpath)
        self.log.debug('cmd = %s' % cmd)

        # launch command
        ret = subprocess.call(shlex.split(cmd))

        # wait for some time until pid file appears
        if not sleep_until(lambda: os.path.exists(self.pid_fpath)):
            raise Exception('Failed to execute vw process. Pid file not found.')

        # check if command was successfull (check pid file)
        print open(self.pid_fpath).read(), ret
        return ret

        # initilize socket for communication

    def load_options(self):
        if os.path.exists(self.options_fpath):
            return eval(open(self.options_fpath).read())
        else:
            return dict(VWOPTIONS)

    def save_options(self):
        open(self.options_fpath, 'w').write(repr(self.options))

    def unload(self):
        pass

    def destroy(self):
        pid = self.kill_vw_processes()
        if not sleep_until(lambda: not is_process_running(pid)):
            raise Exception('unable to kill vw process with pid %s' % pid)

        shutil.rmtree(self.data_dir)

def ensurevw(fn):
    def wfn(self, vw, *args, **kwargs):
        data_dir = os.path.join(self.data_dir, vw)

        if vw not in self.vws and not VW.exists(vw, data_dir):
            raise Exception('vw "%s" does not exist' % vw)

        if vw not in self.vws:
            self.vws[vw] = VW(vw, data_dir, log=self.log)

        return fn(self, self.vws[vw], *args, **kwargs)
    return wfn

class VWAPI(object):
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.vws = {}

    def _check_options(self, options):
        extra_options = set(options.iterkeys()) - set(VWOPTIONS.iterkeys())
        if extra_options:
            raise Exception('Unexpected options: %s' % ','.join(extra_options))

    def create(self, name, options=None):
        options = options or VWOPTIONS
        self._check_options(options)
        data_dir = os.path.join(self.data_dir, name)

        if name in self.vws or VW.exists(name, data_dir):
            raise Exception('vw model "%s" exists already' % name)

        os.makedirs(data_dir)
        self.vws[name] = VW(name, data_dir, self.log, options)

    @ensurevw
    def dummy(self, vw):
        #FIXME: remove this later
        return True

    @ensurevw
    def destroy(self, vw):
        vw.destroy()
        del self.vws[vw.name]

class VWServer(RPCServer):
    NAME = 'VWServer'
    DESC = 'Vowpal Wabbit Server'

    def __init__(self, *args, **kwargs):
        super(VWServer, self).__init__(*args, **kwargs)

        # make data dir if not already present
        self.data_dir = os.path.abspath(self.args.data_dir)
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

    def prepare_api(self):
        return VWAPI(self.args.data_dir)

    def define_args(self, parser):
        parser.add_argument('data_dir', type=str, metavar='data-dir',
            help='Directory path where data is stored')

if __name__ == '__main__':
    VWServer().start()
