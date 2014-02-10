__author__ = 'michael'

import os
import sys
import logging
import Queue
import time
import threading
import zookeeper


ZOO_OPEN_ACL_UNSAFE = {"perms": 0x1f, "scheme": "world", "id": "anyone"}

zk_logger = logging.getLogger('py_zkclient')
# logger starts
logger = logging.getLogger('putin')
#let me show you a very nice log out format, such as [<process_id>:<thread_id>:<log_level>:<time>:<file>(<lineno>)] msg
LOG_FORMAT = "[%(process)d:%(thread)d:%(levelname)s:%(asctime)s:%(filename)s(%(lineno)d)] %(message)s"
logging.basicConfig(level=logging.DEBUG, stream=sys.stdout, format=LOG_FORMAT, datefmt='%Y%m%d%H%M%S')
# logger ends


class ZkException(Exception):
    def __init__(self):
        super(ZkException, self).__init__()


class Listener(object):
    def __init__(self, znode_path):
        self.znode_path = znode_path

    def get_znode_path(self):
        return self.znode_path


class NodeDataListener(Listener):
    def __init__(self, path):
        super(NodeDataListener, self).__init__(path)

    def Update(self, value):
        pass

    def Delete(self):
        pass


class NodeChildrenListener(Listener):
    def __init__(self, path):
        super(NodeChildrenListener, self).__init__(path)

    def Update(self, children_name_list):
        pass

# TODO: ZkClient singleton GetInstance() , like tornaode impl
CV = threading.Condition()


class ZkClient(object):
    # host such as '127.0.0.1:2181' or '192.168.20.1:2181,192.168.20.2:2181'
    def __init__(self, host, zk_log_path='/dev/null'):
        self.host = host
        self.handle = None
        zookeeper.set_debug_level(zookeeper.LOG_LEVEL_WARN)
        log_stream = None
        try:
            log_stream = open(zk_log_path, 'a+')
        except IOError, e:
            zk_logger.error(e)
        if log_stream:
            zookeeper.set_log_stream(log_stream)
        else: #zookeeper.set_log_stream(sys.stdout) # default zk log output to stderr, so I change it from stderr to stdout
            zookeeper.set_log_stream(sys.stdout)

        def EventWatcher(handle, type, state, path):
            zk_logger.info('handle: %d, type: %d, state: %d, path: %s' % (handle, type, state, path))
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
                zk_logger.info('SHIT_EVENT')


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
            zk_logger.debug(self.node_data_listeners)
            if isinstance(listener, NodeDataListener):
                path = listener.get_znode_path()
                zk_logger.debug(path)
                if path in self.node_data_listeners:
                    self.node_data_listeners[path].append(listener)
                else:
                    self.node_data_listeners[path] = [listener]
            zk_logger.debug(self.node_data_listeners)
        except TypeError, err:
            zk_logger.error(err)
        finally:
            self.node_data_listener_lock.release()

    def AddNodeChildrenListener(self, listener):
        try:
            self.children_listener_lock.acquire()
            if isinstance(listener, NodeChildrenListener):
                path = listener.get_znode_path()
                if path in self.children_listeners:
                    self.children_listeners[path].append(listener)
                else:
                    self.children_listeners[path] = [listener]
            zk_logger.debug(self.children_listeners)
        except TypeError, err:
            zk_logger.error(err)
        finally:
            self.children_listener_lock.release()


    def Create(self, path, value, flag):
        CV.acquire()
        try:
            return zookeeper.create(self.handle, path, value, [ZOO_OPEN_ACL_UNSAFE], flag)
        except zookeeper.ZooKeeperException, e:
            zk_logger.error(e)
        finally:
            CV.release()

    def Get(self, path, watch):
        CV.acquire()
        try:
            (data, stat) = zookeeper.get(self.handle, path, self.watcher_fn if watch else None)
            return (data, stat)
        except zookeeper.ZooKeeperException, e:
            zk_logger.error(e)
        finally:
            CV.release()

    def Set(self, path, value, version):
        CV.acquire()
        try:
            return zookeeper.set(self.handle, path, value, version)
        except zookeeper.ZooKeeperException, e:
            zk_logger.error(e)
        finally:
            CV.release()

    def GetChildren(self, path, watch):
        CV.acquire()
        try:
            return zookeeper.get_children(self.handle, path, self.watcher_fn if watch else None)
        except zookeeper.ZooKeeperException, e:
            zk_logger.error(e)
        finally:
            CV.release()

    def Delete(self, path):
        CV.acquire()
        try:
            return zookeeper.delete(self.handle, path)
        except zookeeper.ZooKeeperException, e:
            zk_logger.error(e)
        finally:
            CV.release()

    def Exist(self, path, watch):
        CV.acquire()
        try:
            return zookeeper.exists(self.handle, path, self.watcher_fn if watch else None)
        except zookeeper.ZooKeeperException, e:
            zk_logger.error(e)
        finally:
            CV.release()


    def _UpdateChildren(self, path):
        new_children = None
        try:
            new_children = self.GetChildren(path, True)
        except Exception, e:
            zk_logger.error(e)
        if not new_children:
            zk_logger.error("ZkClient._UpdateChildren() path:%s, new_children is None" % (path))
            return True
        if path not in self.children_listeners:
            zk_logger.error("ZkClient._UpdateChildren() not found listener array whose index is %s" % (path))
            return True
        try:
            self.children_listener_lock.acquire()
            result_array = []
            for listener in self.children_listeners[path]:
                result_array.append(listener.Update(new_children))
            return False not in result_array
        finally:
            self.children_listener_lock.release()

    def _UpdateNode(self, path):
        new_data_stat = None
        try:
            new_data_stat = self.Get(path, True)
        except Exception, e:
            zk_logger.error(e)
        if not new_data_stat:
            zk_logger.error("ZkClient._UpdateNode() path:%s , new_data_stat is None" % (path))
            return True
        if path not in self.node_data_listeners:
            zk_logger.error("ZkClient._UpdateNode() not found listener array whose index is %s" % (path))
            return True
        try:
            self.node_data_listener_lock.acquire()
            result_array = []
            for listener in self.node_data_listeners[path]:
                result_array.append(listener.Update(new_data_stat[0]))
            return False not in result_array
        finally:
            self.node_data_listener_lock.release()

    def _DeleteNode(self, path):
        if path not in self.node_data_listeners:
            zk_logger.error("ZkClient._DeleteNode() not found listener array whose index is %s" % (path))
            return True
        try:
            self.node_data_listener_lock.acquire()
            result_array = []
            for listener in self.node_data_listeners[path]:
                result_array.append(listener.Delete())
            return False not in result_array
        finally:
            self.node_data_listener_lock.release()

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

    def get_type(self):
        return self.type

    def get_count(self):
        return self.count

    def Inc(self):
        self.count += 1


class NotifyTask(object):
    def __init__(self):
        #super(NotifyTask, self).__init__(name='NotifyTask')
        self.zk_client = None
        self.messages = Queue.Queue()

    def set_zkclient(self, client):
        self.zk_client = client

    def AddMessage(self, msg):
        self.messages.put(msg)

    def __call__(self, *args, **kwargs):
        while True:
            msg = self.messages.get()
            if not self.zk_client:
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

'''
if __name__ == '__main__':
    zk_client = ZkClient('127.0.0.1:2181')
    path = '/test4'
    # print zk_client.Create(path, '123', zookeeper.EPHEMERAL)
    # print zk_client.Exist(path, True)
    # print zk_client.Delete(path)
    print zk_client.GetChildren("/test13", False)
    time.sleep(3)
'''
