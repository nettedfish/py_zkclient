__author__ = 'michael'

import unittest
import time
import zkclient


class MyNodeDataListener(zkclient.NodeDataListener):
    def __init__(self, path, id):
        super(MyNodeDataListener, self).__init__(path)
        self.id = id

    def Update(self, value):
        print 'MyNodeDataListener.Update() path:%s\tnewvalue:%s, id:%s' % (self.get_znode_path(), value, self.id)
        return True

    def Delete(self):
        print "MyNodeDataListener.Delete() path:%s, id:%d" % (self.get_znode_path(), self.id)
        return True


class MyNodeChildrenListener(zkclient.NodeChildrenListener):
    def __init__(self, path, id):
        super(MyNodeChildrenListener, self).__init__(path)
        self.id = id

    def Update(self, children_name_list):
        print 'MyNodeChildrenListener.Update() children_name_list:%s, id:%d' % (children_name_list, self.id)
        return True



class MyTestCase(unittest.TestCase):
    def test_something(self):
        self.assertEqual(True, False)

    def setUp(self):
        pass
        #self.zkclient = zkclient.ZkClient('127.0.0.1:2181')
        #self.test_path = '/test4'

    def run(self, result=None):
        pass

    def tearDown(self):
        pass

def Main():
    print 'test main'
    zk_client = zkclient.ZkClient('127.0.0.1:2181')
    path = "/test4"
    print zk_client.Get(path, True)
    my1 = MyNodeDataListener(path, 1)
    # my2 = MyNodeDataListener(path, 2)
    zk_client.AddNodeDataListener(my1)
    # zk_client.AddNodeDataListener(my2)
    l1 = MyNodeChildrenListener(path, 100)
    # l2 = MyNodeChildrenListener(path, 101)
    zk_client.AddNodeChildrenListener(l1)
    print zk_client.GetChildren(path, True)
    time.sleep(100)


if __name__ == '__main__':
    #unittest.main()
    Main()
