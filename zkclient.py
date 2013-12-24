__author__ = 'michael'

import os
import sys
import logging
import Queue
import time
import threading
import zookeeper


ZOO_OPEN_ACL_UNSAFE = {"perms": 0x1f, "scheme": "world", "id": "anyone"}

zk_logger = logging.getLogger('zkclient')


class ZkException(Exception):
    def __init__(self):
        super(ZkException, self).__init__()


class Listener(object):
    def __init__(self, znode_path):
        self.znode_path = znode_path

    def get_znode_path(self):
        self.znode_path


class NodeDataListener(Listener):
    def __init__(self, path):
        super(NodeDataListener, self).__init__(path)

    def Update(self, value):
        pass

    def Delete(self):
        pass


class NodeChildrenListener(Listener):
    def __init__(self, path):
        super(NodeChildrenListener, self).__init__(self)

    def Update(self, children_name_list):
        pass

# TODO: ZkClient singleton GetInstance() , like tornaode impl
class ZkClient(object):
    def __init__(self, host): # host such as '127.0.0.1:2181' or '192.168.20.1:2181,192.168.20.2:2181'
        CV = threading.Condition()
        self.host = host
        self.handle = None
        #zookeeper.set_log_stream(sys.stdout) # default zk log output to stderr, so I change it from stderr to stdout
        def EventWatcher(handle, type, state, path):
            print 'handle: %d, type: %d, state: %d, path: %s' % (handle, type, state, path)
            if type == zookeeper.SESSION_EVENT:
                CV.acquire()
                if state == zookeeper.CONNECTED_STATE:
                    self.connected = True
                    self.handle = handle
                elif state == zookeeper.EXPIRED_SESSION_STATE:
                    # TODO reconnect, refresh
                    pass
                elif state == zookeeper.CONNECTING_STATE:
                    pass
                CV.notify()
                CV.release()
            elif type == zookeeper.CHANGED_EVENT:
                if path:
                    self.notify_task.AddMessage(Message(path, Message.NODE_DATA_CHANGED))
                    try:
                        self.Exist(path, True)
                    except Exception, e:
                        zk_logger.error(e)
            elif type == zookeeper.CHILD_EVENT:
                if path:
                    self.notify_task.AddMessage(Message(path, Message.NODE_CHILDREN_CHANGED))
                    try:
                        self.Exist(path, True)
                    except Exception, e:
                        zk_logger.error(e)
            elif type == zookeeper.CREATED_EVENT:
                if path:
                    self.notify_task.AddMessage(Message(path, Message.NODE_CREATED))
                    try:
                        self.Exist(path, True)
                    except Exception, e:
                        zk_logger.error(e)
            elif type == zookeeper.DELETED_EVENT:
                if path:
                    self.notify_task.AddMessage(Message(path, Message.NODE_DELETED))
                    try:
                        self.Exist(path, True)
                    except Exception, e:
                        zk_logger.error(e)
            else:
                print 'SHIT_EVENT'


        CV.acquire()
        self.watcher_fn = EventWatcher
        ret = zookeeper.init(self.host, self.watcher_fn)
        CV.wait()
        CV.release()

        self.node_data_listeners = {} # string to listener array
        self.node_data_listener_lock = threading.Lock()
        self.children_listeners = {}  # string to listener array
        self.children_listener_lock = threading.Lock()

        self.notify_task = NotifyTask()
        self.notify_task.set_zkclient(self)
        self.notify_thread = threading.Thread(target=self.notify_task)
        self.notify_thread.setDaemon(True)
        self.notify_thread.start()


    def AddNodeDataListener(self, listener):
        try:
            self.node_data_listener_lock.acquire()
            if issubclass(listener, NodeDataListener):
                path = listener.get_znode_path()
                if self.node_data_listeners.has_key(path):
                    self.node_data_listeners[path].append(listener)
                else:
                    self.node_data_listeners[path] = [listener]
        except TypeError, err:
            zk_logger.error(err)
        finally:
            self.node_data_listener_lock.release()

    def AddNodeChildrenListener(self, listener):
        try:
            self.children_listener_lock.acquire()
            if issubclass(listener, NodeChildrenListener):
                path = listener.get_znode_path()
                if self.children_listeners.has_key(path):
                    self.children_listeners[path].append(listener)
                else:
                    self.children_listeners[path] = [listener]
        except TypeError, err:
            zk_logger.error(err)
        finally:
            self.children_listener_lock.release()


    def Create(self, path, value, flag):
        try:
            zookeeper.create(self.handle, path, value, [ZOO_OPEN_ACL_UNSAFE], flag)
        except zookeeper.ZooKeeperException, e:
            print str(e)

    def Get(self, path, watch):
        try:
            (data, stat) = zookeeper.get(self.handle, path, self.watcher_fn if watch else None)
            return (data, stat)
        except zookeeper.ZooKeeperException, e:
            print str(e)
            return None

    def Set(self, path, value, version):
        zookeeper.set(self.handle, path, value, version)

    def GetChildren(self, path, watch):
        try:
            return zookeeper.get_children(self.handle, path, self.watcher_fn if watch else None)
        except zookeeper.ZooKeeperException, e:
            print str(e)
            return []

    def Delete(self, path):
        zookeeper.delete(self.handle, path)

    def Exist(self, path, watch):
        zookeeper.exists(self.handle, path, self.watcher_fn if watch else None)

    def _UpdateChildren(self, path):
        # TODO
        pass

    def _UpdateNode(self, path):
        # TODO
        pass

    def _DeleteNode(self, path):
        # TODO
        pass

    def _UpdateAll(self):
        # TODO
        pass

class Message(object):
    NODE_CREATED = 100
    NODE_DELETED = 101
    NODE_CHILDREN_CHANGED = 102
    NODE_DATA_CHANGED = 103
    NODE_REFRESH = 104
    MAX_UPDATED_COUNT = 3

    def __init__(self, path, type):
        self.path = path
        self.type = type
        self.count = 1

    def get_path(self):
        return self.path

    def get_type(self, type):
        return self.type

    def get_count(self):
        return self.count

    def Inc(self):
        self.count += 1


class NotifyTask():
    def __init__(self):
        super(NotifyTask, self).__init__(name='NotifyTask')
        self.zk_client = None
        self.messages = Queue.Queue()
    
    def set_zkclient(self, client):
        self.zk_client = client

    def AddMessage(self, msg):
        self.messages.put(msg)

    def __call__(self, *args, **kwargs):
        while True:
            msg = self.messages.get()
            if self.zk_client:
                zk_logger.error('NotifyTask has no zkclient')
                continue
            if msg.get_count() >= Message.MAX_UPDATED_COUNT:
                zk_logger.error('Message cannot be updated, node_path: %s' % msg.get_path())
                continue
            type = msg.get_type()
            ret = False
            if type == Message.NODE_CHILDREN_CHANGED:
                ret = self.zk_client._UpdateChildren(msg.get_path())
                pass
            elif type == Message.NODE_CREATED:
                ret = self.zk_client._UpdateNode(msg.get_path())
                pass
            elif type == Message.NODE_DELETED:
                ret = self.zk_client._DeleteNode(msg.get_path())
                pass
            elif type == Message.NODE_DATA_CHANGED:
                ret = self.zk_client._UpdateNode(msg.get_path())
                pass
            elif type == Message.NODE_REFRESH:
                ret = self.zk_client._UpdateAll()
                pass
            if not ret:
                msg.Inc()
                self.messages.put(msg)


if __name__ == '__main__':
    zk_client = ZkClient('127.0.0.1:2181')
    path = '/test'
    val = zk_client.Get(path, True)
    (data, stat) = val
    print data
    time.sleep(100)
