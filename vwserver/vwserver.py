from gevent import monkey; monkey.patch_all()

import os
import sys
import time
import shlex
import shutil
import subprocess
import signal
import socket
import threading
import copy

import psutil
import tornado
import websocket
from decorator import decorator
from funcserver import RPCServer, RPCClient

# As we are using gevent for IO parallelization (network RPC requests) and
# gevent does not play well with process spawning, we need to make sure
# that the process spawned is a daemon otherwise the event loop gets stuck
# Vowpal wabbit does support daemonizing however that isn't working in
# active learning mode as of version 7.7. To overcome this we are using
# a Vowpal wabbit wrapping python script that daemonizes and execs into
# the Vowpal wabbit binary
VWWRAPPER = os.path.join(os.path.dirname(__file__), 'vwdaemon.py')

VWOPTIONS = {
    'passes': 3,
    'bit_precision': 27,
    'active_learning': False,
    'active_mellowness': 8,
}

def get_vwdaemon_pid(port):
    '''
    Finds the vwwrapper daemon process that is listening on @port
    and returns pid of that process
    '''
    for pid in psutil.pids():
        try:
            p = psutil.Process(pid)
            conns = p.connections()
        except psutil.AccessDenied:
            continue
        conns = [c.laddr for c in conns if c.status == 'LISTEN']
        ports = [_port for _, _port in conns]
        if port in ports and p.parent().pid == 1:
            return pid

    return None

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

    def __init__(self, vw, on_fatal_failure=None, on_connect=None):
        self.vw = vw
        self.log = vw.log
        self.port = vw.port
        self.lock = threading.RLock()
        self.on_fatal_failure = on_fatal_failure
        self.on_connect = on_connect

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
            if self.on_connect: self.on_connect()
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

    def __init__(self, name, data_dir, vw_binary, log, options=None, on_fatal_failure=None):
        self.log = log
        self.name = name
        self.data_dir = data_dir
        self.vw_binary = vw_binary
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

    def make_options(self):
        o = []
        for k, v in self.options.iteritems():
            # boolean value: active=True becomes --active
            if isinstance(v, bool):
                if v is True:
                    o.append('--%s' % k)

            elif isinstance(v, str):
                o.append('--%s "%s"' % (k, v.replace('"', r'\"')))

            else:
                o.append('--%s %s' % (k, v))

        return o

    def load_vw(self):
        # ensure cache file
        open(self.cache_fpath, 'a+').close()

        # prepare dummy input (required to force vw to use empty cache
        # to start with)
        open(self.dummy_input_fpath, 'w').close()

        self.port = get_free_port()

        # user-specifiable options
        opts = self.make_options()

        # model file option
        if os.path.exists(self.model_fpath):
            opts.append('--initial_regressor %s' % self.model_fpath)
        else:
            opts.append('--final_regressor %s' % self.model_fpath)

        # standard options
        # NOTE: disabling cache because it is causing issues
        # with online training
        opts.extend(['--no_stdin', '--save_resume', '--quiet',
                    '--num_children %s' % self.NUM_CHILD_PROCESSES,
                    #'--cache_file %s' % self.cache_fpath,
                    '--port %s' % self.port])

        # construct vw command
        cmd = '%s %s %s %s %s %s' % (sys.executable, VWWRAPPER, self.pid_fpath,
            self.vw_binary, ' '.join(opts), self.dummy_input_fpath)
        self.log.debug('cmd = %s' % cmd)

        # launch command
        ret = subprocess.call(shlex.split(cmd))

        # wait for some time until pid file appears
        if not sleep_until(lambda: os.path.exists(self.pid_fpath)):
            raise Exception('Failed to execute vw process. Pid file not found.')

        # initilize socket for communication
        # NOTE: Upon successful connection, we are finding the pid
        # of the daemon process that is found to be listening on
        # the correct port number. We are doing this and overwriting the pid
        # written into pid file by VWWRAPPER because for some reason
        # the pid is off by one!
        self.sock = VWSocket(self, on_fatal_failure=self.on_fatal_failure,
            on_connect=lambda: open(self.pid_fpath, 'w').write(str(get_vwdaemon_pid(self.port))))

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

@decorator
def ensurevw(fn, vw, *args, **kwargs):
    data_dir = os.path.join(self.data_dir, vw)

    if vw not in self.vws and not VW.exists(vw, data_dir):
        raise Exception('vw "%s" does not exist' % vw)

    if vw not in self.vws:
        self.vws[vw] = VW(vw, self.vw_binary, data_dir, log=self.log,
            on_fatal_failure=lambda: self.unload(vw))

    return fn(self, self.vws[vw], *args, **kwargs)

class VWAPI(object):
    def __init__(self, data_dir, vw_binary):
        self.data_dir = data_dir
        self.vw_binary = vw_binary
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
        self.vws[name] = VW(name, data_dir, self.vw_binary, self.log, options,
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
        self.vw_binary = os.path.abspath(self.args.vw_binary)
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

    def prepare_api(self):
        return VWAPI(self.data_dir, self.vw_binary)

    def prepare_handlers(self):
        return [('/ws/vw/([^/]+)', WSVWHandler)]

    def define_args(self, parser):
        parser.add_argument('data_dir', type=str, metavar='data-dir',
            help='Directory path where data is stored')
        parser.add_argument('vw_binary', type=str, metavar='vw-binary',
            help='Absolute path of vw executable file')

if __name__ == '__main__':
    VWServer().start()
