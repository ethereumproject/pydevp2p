import time
from devp2p import peermanager
from devp2p.app import BaseApp
from devp2p.protocol import BaseProtocol, SubProtocolError
from devp2p.service import WiredService, BaseService
from devp2p.discovery import NodeDiscovery
from devp2p.crypto import privtopub as privtopub_raw, sha3
from devp2p.utils import host_port_pubkey_to_uri, update_config_with_defaults, colors, COLOR_END
import rlp
import gevent
from gevent.event import Event
import signal
import copy
import sys
from devp2p.p2p_protocol import ConnectionMonitor
import ethereum.slogging as slogging
slogging.configure(config_string=':debug,p2p.discovery:info')
log = slogging.get_logger('app')

ConnectionMonitor.ping_interval = 2**256  # deactive pings


class Token(rlp.Serializable):

    "Object with the information to update a decentralized counter"
    fields = [
        ('counter', rlp.sedes.big_endian_int),
        ('sender', rlp.sedes.binary)
    ]

    def __init__(self, counter=0, sender=''):
        assert isinstance(counter, int)
        assert isinstance(sender, bytes)
        super(Token, self).__init__(counter, sender)

    @property
    def hash(self):
        return sha3(rlp.encode(self))

    def __repr__(self):
        try:
            return '<%s(counter=%d hash=%s)>' % (self.__class__.__name__, self.counter,
                                                 self.hash.encode('hex')[:4])
        except:
            return '<%s>' % (self.__class__.__name__)


class ExampleProtocol(BaseProtocol):
    protocol_id = 1
    network_id = 0
    max_cmd_id = 15  # FIXME
    name = 'example'
    version = 1

    def __init__(self, peer, service):
        # required by P2PProtocol
        self.config = peer.config
        BaseProtocol.__init__(self, peer, service)

    class token(BaseProtocol.command):

        """
        message sending a token and a nonce
        """
        cmd_id = 0
        sent = False

        structure = [
            ('token', Token)
        ]


class DuplicatesFilter(object):

    def __init__(self, max_items=1024):
        self.max_items = max_items
        self.filter = list()

    def update(self, data):
        "returns True if unknown"
        if data not in self.filter:
            self.filter.append(data)
            if len(self.filter) > self.max_items:
                self.filter.pop(0)
            return True
        else:
            self.filter.append(self.filter.pop(0))
            return False

    def __contains__(self, v):
        return v in self.filter


class ExampleService(WiredService):

    # required by BaseService
    name = 'exampleservice'
    default_config = dict(example=dict(num_participants=1))

    # required by WiredService
    wire_protocol = ExampleProtocol  # create for each peer

    def __init__(self, app):
        self.config = app.config
        self.broadcast_filter = DuplicatesFilter()
        self.counter = 0
        self.address = privtopub_raw(self.config['node']['privkey_hex'].decode('hex'))
        super(ExampleService, self).__init__(app)

    def start(self):
        super(ExampleService, self).start()

    def log(self, text, **kargs):
        node_num = self.config['node_num']
        msg = ' '.join([
            colors[node_num % len(colors)],
            "NODE%d" % node_num,
            text,
            (' %r' % kargs if kargs else ''),
            COLOR_END])
        log.debug(msg)

    def on_wire_protocol_stop(self, proto):
        assert isinstance(proto, self.wire_protocol)
        self.log('----------------------------------')
        self.log('on_wire_protocol_stop', proto=proto)

    def broadcast(self, obj, origin=None):
        """
        """
        fmap = {Token: 'token'}
        if not self.broadcast_filter.update(obj.hash):
            self.log('already broadcasted', obj=obj)
            return
        self.log('broadcasting', obj=obj)
        bcast = self.app.services.peermanager.broadcast
        bcast(ExampleProtocol, fmap[type(obj)], args=(obj,),
              exclude_peers=[origin.peer] if origin else [])

    # application logic

    def on_wire_protocol_start(self, proto):
        self.log('----------------------------------')
        self.log('on_wire_protocol_start', proto=proto, peers=self.app.services.peermanager.peers)
        assert isinstance(proto, self.wire_protocol)
        # register callbacks
        proto.receive_token_callbacks.append(self.on_receive_token)

        # wait for other nodes
        gevent.sleep(1)
        self.send_token()

    def on_receive_token(self, proto, token):
        assert isinstance(token, Token)
        assert isinstance(proto, self.wire_protocol)
        self.log('----------------------------------')
        self.log('on_receive token', token=token, proto=proto)
        if token.hash in self.broadcast_filter:
            self.log('known token')
            return
        if token.counter != self.counter + 1:
            self.log('invalid token, not in sync')
            return
        self.counter = token.counter
        self.send_token()

    def send_token(self):
        turn = self.counter % self.config['num_nodes']
        if turn != self.config['node_num']:
            return
        token = Token(counter=self.counter + 1, sender=self.address)
        self.log('sending token', token=token)
        self.broadcast(token)

services = [NodeDiscovery, peermanager.PeerManager, ExampleService]


class ExampleApp(BaseApp):
    client_name = 'exampleapp'
    version = '0.1'
    client_version = '%s/%s/%s' % (version, sys.platform,
                                   'py%d.%d.%d' % sys.version_info[:3])
    client_version_string = '%s/v%s' % (client_name, client_version)
    default_config = dict(BaseApp.default_config)
    default_config['client_version_string'] = client_version_string
    default_config['post_app_start_callback'] = None


def mk_privkey(seed):
    return sha3(seed)


def create_app(node_num, config):
    num_nodes = config['num_nodes']
    assert node_num < num_nodes
    base_port = config['base_port']
    seed = config['seed']
    config = copy.deepcopy(config)
    config['node_num'] = node_num

    # create this node priv_key
    config['node']['privkey_hex'] = mk_privkey('%d:udp:%d' % (seed, node_num)).encode('hex')
    # set ports based on node
    config['discovery']['listen_port'] = base_port + node_num
    config['p2p']['listen_port'] = base_port + node_num
    config['p2p']['min_peers'] = min(10, num_nodes - 1)
    config['p2p']['max_peers'] = num_nodes
    config['client_version_string'] = 'NODE{}'.format(node_num)

    app = ExampleApp(config)

    # register services
    for service in services:
        assert issubclass(service, BaseService)
        if service.name not in app.config['deactivated_services']:
            assert service.name not in app.services
            service.register_with_app(app)
            assert hasattr(app.services, service.name)

    return app


def serve_until_stopped(apps):
    for app in apps:
        app.start()
        if app.config['post_app_start_callback'] is not None:
            app.config['post_app_start_callback'](app)

    # wait for interrupt
    evt = Event()
    gevent.signal(signal.SIGQUIT, evt.set)
    gevent.signal(signal.SIGTERM, evt.set)
    gevent.signal(signal.SIGINT, evt.set)
    evt.wait()

    # finally stop
    for app in apps:
        app.stop()


def run(num_nodes=3, seed=0):
    gevent.get_hub().SYSTEM_ERROR = BaseException
    base_port = 29870

    # get bootstrap node (node0) enode
    bootstrap_node_privkey = mk_privkey('%d:udp:%d' % (seed, 0))
    bootstrap_node_pubkey = privtopub_raw(bootstrap_node_privkey)
    enode = host_port_pubkey_to_uri(b'0.0.0.0', base_port, bootstrap_node_pubkey)

    # prepare config
    base_config = dict()
    for s in services + [ExampleApp]:
        update_config_with_defaults(base_config, s.default_config)

    base_config['discovery']['bootstrap_nodes'] = [enode]
    base_config['seed'] = seed
    base_config['base_port'] = 29870
    base_config['num_nodes'] = num_nodes

    # prepare apps
    apps = []
    for node_num in range(num_nodes):
        app = create_app(node_num, base_config)
        apps.append(app)

    # start apps
    serve_until_stopped(apps)


if __name__ == '__main__':
    run()
