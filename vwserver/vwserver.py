from gevent import monkey; monkey.patch_socket()

import time
import os
import shlex
import shutil
import subprocess
import signal
import socket
import threading
import copy
from functools import wraps

import tornado
import websocket
from funcserver import RPCServer, RPCClient

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

def sleep_until(fn, timeout=25.0):
    '''
    Sleeps until fn returns True. Performing sleeping
    in incrementing sections based on exponential
    backoff
    '''
    telapsed = 0

    for t in (.1, .2, .4, .8, 1.6, 3.2, 6.4, 12.8):
        if not fn(): time.sleep(t)
        telapsed += t
        if telapsed > timeout: break

    return fn()

class VWSocket(object):
    CHUNK_SIZE = 4096

    def __init__(self, vw, on_fatal_failure=None):
        self.vw = vw
        self.log = vw.log
        self.port = vw.port
        self.lock = threading.RLock()
        self.on_fatal_failure = on_fatal_failure

        sleep_until(self.connect, timeout=5.0)

    def _recvlines(self, num):
        n = 0
        data = []

        while n < num:
            s = self.sock.recv(self.CHUNK_SIZE)
            if '\n' in s:
                last, s = s.rsplit('\n', 1)
                data.append(last)

                data = ''.join(data).split('\n')
                for line in data:
                    n += 1
                    yield line

                data = [s]

    def connect(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.connect(('localhost', self.port))
            return True
        except socket.error:
            return False

    def reconnect(self):
        if not self.connect():
            if self.on_fatal_failure:
                self.on_fatal_failure()

        return True

    def close(self):
        self.sock.close()

    def send_commands(self, commands, num_responses=None):
        num_responses = num_responses if num_responses is not None else len(commands)

        with self.lock:
            try:
                msg = '\n'.join(commands) + '\n'
                self.sock.sendall(msg)

                if num_responses:
                    return list(self._recvlines(len(commands)))
            except socket.error:
                self.reconnect()
                raise

class VW(object):
    NUM_CHILD_PROCESSES = 8

    def __init__(self, name, data_dir, log, options=None, on_fatal_failure=None):
        self.log = log
        self.name = name
        self.data_dir = data_dir
        self.port = 0
        self.sock = None
        self.on_fatal_failure = on_fatal_failure

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
        # NOTE: disabling cache because it is causing issues
        # with online training
        opts.extend(['--daemon', '--no_stdin', '--save_resume', '--quiet',
                    '--num_children %s' % self.NUM_CHILD_PROCESSES,
                    '--pid_file %s' % self.pid_fpath,
                    #'--cache_file %s' % self.cache_fpath,
                    '--port %s' % self.port])

        # construct vw command
        cmd = 'vw %s %s' % (' '.join(opts), self.dummy_input_fpath)
        self.log.debug('cmd = %s' % cmd)

        # launch command
        ret = subprocess.call(shlex.split(cmd))

        # wait for some time until pid file appears
        if not sleep_until(lambda: os.path.exists(self.pid_fpath)):
            raise Exception('Failed to execute vw process. Pid file not found.')

        # initilize socket for communication
        self.sock = VWSocket(self, on_fatal_failure=self.on_fatal_failure)

    def load_options(self):
        if os.path.exists(self.options_fpath):
            return eval(open(self.options_fpath).read())
        else:
            return dict(VWOPTIONS)

    def save_options(self):
        open(self.options_fpath, 'w').write(repr(self.options))

    def save(self):
        self.sock.send_commands(['save'], num_responses=0)

    def train(self, examples):
        return self.sock.send_commands(examples)

    def predict(self, items):
        return self.sock.send_commands(items)

    def unload(self):
        self.sock.close()

        pid = self.kill_vw_processes()
        if not sleep_until(lambda: not is_process_running(pid)):
            raise Exception('unable to kill vw process with pid %s' % pid)

    def destroy(self):
        self.unload()
        shutil.rmtree(self.data_dir)

def ensurevw(fn):
    @wraps(fn)
    def wfn(self, vw, *args, **kwargs):
        data_dir = os.path.join(self.data_dir, vw)

        if vw not in self.vws and not VW.exists(vw, data_dir):
            raise Exception('vw "%s" does not exist' % vw)

        if vw not in self.vws:
            self.vws[vw] = VW(vw, data_dir, log=self.log,
                on_fatal_failure=lambda: self.unload(vw))

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

    def show_options(self):
        '''
        Shows the allowed options and their default values
        '''
        return copy.deepcopy(VWOPTIONS)

    def create(self, name, options=None):
        '''
        Creates a new VW model with @name and using @options
        '''
        options = options or VWOPTIONS
        self._check_options(options)
        data_dir = os.path.join(self.data_dir, name)

        if name in self.vws or VW.exists(name, data_dir):
            raise Exception('vw model "%s" exists already' % name)

        os.makedirs(data_dir)
        self.vws[name] = VW(name, data_dir, self.log, options,
            on_fatal_failure=lambda: self.unload(name))

    def unload(self, vw):
        '''
        Unloads a VW model from memory. This does not
        destroy the model from disk and so it can be
        loaded again later for usage.
        '''
        if vw not in self.vws: return

        vw = self.vws[vw]
        vw.unload()

    @ensurevw
    def destroy(self, vw):
        '''
        Destroy the specified VW model from both memory
        and disk permanently.
        '''
        vw.destroy()
        del self.vws[vw.name]

    def _check_item_format(self, item):
        return not ('\n' in item or '\r' in item)

    def _check_items(self, items):
        for index, item in enumerate(items):
            if not self._check_item_format(item):
                raise Exception('Bad format for item at index %s' % index)

    @ensurevw
    def train(self, vw, examples):
        '''
        Train the @vw model using @examples
        @examples - a list of strings representing example lines
            in the VW format

        returns: a list of response lines as returned by VW
        '''
        self._check_items(examples)
        return vw.train(examples)

    @ensurevw
    def predict(self, vw, items):
        '''
        Perform prediction using @vw model on the provided @items.
        @items - a list of strings representing the input lines
            in the VW format

        returns: a list of response lines as returned by VW
        '''
        self._check_items(items)
        return vw.predict(items)

    @ensurevw
    def save(self, vw):
        '''
        Saves the model learnt so far
        '''
        vw.save()

    def shutdown(self):
        '''
        Stop the server
        '''
        for vw in self.vws.itervalues():
            vw.unload()

        sys.exit(0)

class VWClient(RPCClient):
    pass

class WSVWHandler(tornado.websocket.WebSocketHandler):

    def open(self, vw):
        self.server = self.application.funcserver
        self.vw_name = vw
        self.vw = self.server.api.get(vw, None)

        if self.vw is None:
            self.close()

    def on_message(self, msg):
        print 'received: ', msg

    def on_close(self):
        pass

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
        return VWAPI(self.data_dir)

    def prepare_handlers(self):
        return [('/ws/vw/([^/]+)', WSVWHandler)]

    def define_args(self, parser):
        parser.add_argument('data_dir', type=str, metavar='data-dir',
            help='Directory path where data is stored')

if __name__ == '__main__':
    VWServer().start()
