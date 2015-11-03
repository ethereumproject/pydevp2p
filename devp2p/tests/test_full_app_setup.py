import pytest
import os
import signal
import time
from devp2p.app import BaseApp
from devp2p.protocol import BaseProtocol
from devp2p.service import WiredService
from devp2p.crypto import privtopub as privtopub_raw, sha3
from devp2p.utils import colors, COLOR_END
from devp2p import app_helper
import rlp
import gevent
import sys

log = None
NUM_NODES = 3
NODES_PASSED = set()


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


class ExampleService(WiredService):

    # required by BaseService
    name = 'exampleservice'
    default_config = dict(example=dict(num_participants=1))

    # required by WiredService
    wire_protocol = ExampleProtocol  # create for each peer

    def __init__(self, app):
        self.config = app.config
        self.counter = 0
        self.address = privtopub_raw(self.config['node']['privkey_hex'].decode('hex'))
        super(ExampleService, self).__init__(app)

    def start(self):
        super(ExampleService, self).start()

    def log(self, text, **kargs):
        if not log:
            return
        node_num = self.config['node_num']
        msg = ' '.join([
            colors[node_num % len(colors)],
            "%s" % self.config['client_version_string'],
            text,
            (' %r' % kargs if kargs else ''),
            COLOR_END])
        log.debug(msg)

    def on_wire_protocol_stop(self, proto):
        assert isinstance(proto, self.wire_protocol)
        self.log('----------------------------------')
        self.log('on_wire_protocol_stop', proto=proto)

    # application logic

    def on_wire_protocol_start(self, proto):
        assert isinstance(proto, self.wire_protocol)
        my_version = self.config['client_version_string']
        my_peers = self.app.services.peermanager.peers
        assert my_peers

        self.log('----------------------------------')
        self.log('on_wire_protocol_start', proto=proto, my_peers=my_peers)

        # check the peers is not connected to self
        for p in my_peers:
            assert p.remote_client_version != my_version

        # check the peers is connected to distinct nodes
        my_peers_with_hello_received = filter(lambda p: p.remote_client_version != '', my_peers)
        versions = map(lambda p: p.remote_client_version, my_peers_with_hello_received)
        assert len(set(versions)) == len(versions)

        # check if number of peers that received hello is equal to number of min_peers
        if self.config['p2p']['min_peers'] == len(my_peers_with_hello_received):
            NODES_PASSED.add(my_version)
            if len(NODES_PASSED) == NUM_NODES:
                # Stop gevent main loop to stop all apps
                os.kill(os.getpid(), signal.SIGQUIT)


class ExampleApp(BaseApp):
    client_name = 'exampleapp'
    version = '0.1'
    client_version = '%s/%s/%s' % (version, sys.platform,
                                   'py%d.%d.%d' % sys.version_info[:3])
    client_version_string = '%s/v%s' % (client_name, client_version)
    default_config = dict(BaseApp.default_config)
    default_config['client_version_string'] = client_version_string
    default_config['post_app_start_callback'] = None


@pytest.mark.timeout(10)
def test_full_app():
    app_helper.run(ExampleApp, ExampleService, num_nodes=NUM_NODES)
    assert NUM_NODES == len(NODES_PASSED)


if __name__ == "__main__":
    import devp2p.slogging as slogging
    slogging.configure(config_string=':debug,p2p.discovery:info')
    log = slogging.get_logger('app')
