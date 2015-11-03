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
NODES_PASSED_SETUP = set()
NODES_PASSED_BCAST = set()
NODES_PASSED_INC_COUNTER = set()


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

        proto.receive_token_callbacks.append(self.on_receive_token)

        # check if number of peers that received hello is equal to number of min_peers
        if self.config['p2p']['min_peers'] == len(my_peers_with_hello_received):
            NODES_PASSED_SETUP.add(my_version)
            if len(NODES_PASSED_SETUP) == NUM_NODES:
                self.on_all_nodes_ready()

    def killall(self):
        # Test passed. Stop gevent main loop to stop all apps
        os.kill(os.getpid(), signal.SIGQUIT)

    def on_all_nodes_ready(self):
        pass

    def on_receive_token(self, proto, token):
        pass


class ExampleService1(ExampleService):
    def __init__(self, app):
        super(ExampleService1, self).__init__(app)

    def on_all_nodes_ready(self):
        self.killall()


class ExampleService2(ExampleService):
    def __init__(self, app):
        super(ExampleService2, self).__init__(app)

    def on_all_nodes_ready(self):
        self.log("All nodes are OK. Sending token", token=self.config['node_num'])
        token = Token(counter=int(self.config['node_num']), sender=self.address)
        self.broadcast(token)

    def broadcast(self, obj, origin=None):
        fmap = {Token: 'token'}
        self.log('broadcasting', obj=obj)
        bcast = self.app.services.peermanager.broadcast
        bcast(ExampleProtocol, fmap[type(obj)], args=(obj,),
              exclude_peers=[origin.peer] if origin else [])

    def on_receive_token(self, proto, token):
        assert isinstance(token, Token)
        assert isinstance(proto, self.wire_protocol)
        self.log('----------------------------------')
        self.log('on_receive token', token=token, proto=proto)
        NODES_PASSED_BCAST.add(self.config['node_num'])
        if len(NODES_PASSED_BCAST) >= NUM_NODES - 1:
            self.killall()


class ExampleService3(ExampleService):
    def __init__(self, app):
        super(ExampleService3, self).__init__(app)
        self.collected = set()
        self.broadcasted = set()
        self.is_stopping = False
        self.counter_limit = 1024

    def on_all_nodes_ready(self):
        self.send_synchro_token()

    def broadcast(self, obj, origin=None):
        fmap = {Token: 'token'}
        self.log('broadcasting', obj=obj)
        bcast = self.app.services.peermanager.broadcast
        bcast(ExampleProtocol, fmap[type(obj)], args=(obj,),
              exclude_peers=[origin.peer] if origin else [])

    def on_receive_token(self, proto, token):
        assert isinstance(token, Token)
        assert isinstance(proto, self.wire_protocol)
        self.log('----------------------------------')
        self.log('on_receive token {}'.format(token.counter),
                 collected=len(self.collected), broadcasted=len(self.broadcasted))

        assert token.counter not in self.collected

        # NODE0 must send first token to make algorithm work
        if not self.collected and not self.broadcasted and token.counter == 0:
            if self.config['node_num'] == 0:
                self.log("send initial token to the wire.")
                self.try_send_token()
            else:
                self.send_synchro_token()
            return

        if token.counter == 0:
            return

        self.collected.add(token.counter)
        self.log('collected token {}'.format(token.counter))

        if token.counter >= self.counter_limit:
            self.stop_test()
            return

        self.try_send_token()

    def send_synchro_token(self):
        self.log("send synchronization token")
        self.broadcast(Token(counter=0, sender=self.address))

    def try_send_token(self):
        counter = 0 if not self.collected else max(self.collected)
        turn = counter % self.config['num_nodes']
        if turn != self.config['node_num']:
            return
        if counter+1 in self.broadcasted:
            return
        self.broadcasted.add(counter+1)
        token = Token(counter=counter+1, sender=self.address)
        self.log('sending token {}'.format(counter+1), token=token)
        self.broadcast(token)
        if counter+1 == self.counter_limit:
            self.stop_test()

    def stop_test(self):
        if not self.is_stopping:
            self.log("COUNTER LIMIT REACHED. STOP THE APP")
            self.is_stopping = True
            # defer until all broadcast arrive
            gevent.spawn_later(2.0, self.assert_and_stop)

    def assert_and_stop(self):
        self.log("TEST FINISHED", broadcasted=len(self.broadcasted), collected=len(self.collected))
        assert len(self.collected) > len(self.broadcasted)

        for turn in xrange(1, self.counter_limit):
            if (turn-1) % NUM_NODES == self.config['node_num']:
                assert turn in self.broadcasted
            else:
                assert turn in self.collected

        NODES_PASSED_INC_COUNTER.add(self.config['node_num'])

        self.app.stop()
        gevent.spawn_later(2.0, self.killall)

    def killall(self):
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
def test_full_app_setup():
    NODES_PASSED_SETUP.clear()

    app_helper.run(ExampleApp, ExampleService1, num_nodes=NUM_NODES)
    assert NUM_NODES == len(NODES_PASSED_SETUP)


@pytest.mark.timeout(10)
def test_full_app_bcast():
    NODES_PASSED_SETUP.clear()
    NODES_PASSED_BCAST.clear()

    app_helper.run(ExampleApp, ExampleService2,
                   num_nodes=NUM_NODES, min_peers=NUM_NODES-1, max_peers=NUM_NODES-1)

    assert len(NODES_PASSED_BCAST) >= NUM_NODES - 1


@pytest.mark.timeout(20)
def test_full_app_inc_counter():
    NODES_PASSED_SETUP.clear()
    NODES_PASSED_INC_COUNTER.clear()

    app_helper.run(ExampleApp, ExampleService3,
                   num_nodes=NUM_NODES, min_peers=NUM_NODES-1, max_peers=NUM_NODES-1)

    assert len(NODES_PASSED_INC_COUNTER) == NUM_NODES


if __name__ == "__main__":
    import devp2p.slogging as slogging
    slogging.configure(config_string=':debug,p2p:info')
    log = slogging.get_logger('app')
    test_full_app_setup()
    test_full_app_bcast()
    test_full_app_inc_counter()
