import rpyc
import uuid
import threading 
import math
import random
import configparser
import signal
import pickle
import sys
import os
import datetime

from rpyc.utils.server import ThreadedServer
from anytree import NodeMixin, RenderTree, AnyNode
from anytree.search import find

from datanode import DataNode
from collections import defaultdict

def int_handler(signal, frame):
    pickle.dump((MasterService.exposed_Master.file_table,
    MasterService.exposed_Master.block_mapping,
    MasterService.exposed_Master.dir_tree),
    open('fs.img','wb'))
    sys.exit(0)

def set_conf():
  conf=configparser.ConfigParser()
  conf.readfp(open('dfs.conf'))
  MasterService.exposed_Master.block_size = int(conf.get('master','block_size'))
  MasterService.exposed_Master.replication_factor = int(conf.get('master','replication_factor'))
  minions = conf.get('master','minions').split(',')
  for m in minions:
    id,host,port=m.split(":")
    MasterService.exposed_Master.minions[id]=(host,port)

  if os.path.isfile('fs.img'):
    MasterService.exposed_Master.file_table,MasterService.exposed_Master.block_mapping, MasterService.exposed_Master.dir_tree = pickle.load(open('fs.img','rb'))

class MasterService(rpyc.Service):
  class exposed_Master():
    file_table = {}
    block_mapping = {}
    minions = {}
    
    dir_tree = AnyNode(name=".", files=defaultdict(list))

    block_size = 0
    replication_factor = 0

    def exposed_read(self, fname):
        mapping = self.__class__.file_table[fname]
        return mapping
      
    def exposed_change_filepath(self, old_path, new_path):
        self.__class__.file_table[new_path] = self.__class__.file_table.pop(old_path)
        

    def exposed_write(self, dest, size):
        num_blocks = self.calc_num_blocks(size)
        blocks = self.alloc_blocks(dest, num_blocks, size)
        
        return blocks
    
    def exposed_delete_file(self, path):
        dir_path = path[:path.rfind('/')]
        
        file_name = path.split('/')[-1]
        
        dir = self.get_dir_with_path(dir_path)
        
        file_table = dir.files[file_name].blocks
        
        for block in file_table:
            for m in [self.exposed_get_minions()[_] for _ in block[1]]:
                self.delete_block(block[0], m)
              
        del dir.files[file_name]
   
    def delete_block(self, block_uuid, minion):
        host, port = minion
        con = rpyc.connect(host, port = port)
        minion = con.root.Minion()
        
        minion.delete_block(block_uuid)

    def exposed_get_file_table_entry(self, path):
    
        dir_path = path[:path.rfind('/')]
        
        file_name = path.split('/')[-1]
        
        dir = self.get_dir_with_path(dir_path)
        
        if dir != None:
            return dir.files[file_name].blocks
        else:
            raise NameError("Error: {}\n{}\n".format(dir_path, path))
#            return []
        
#        if fname in self.__class__.file_table:
#            return self.__class__.file_table[fname]
#        else:
#            return None

    def exposed_get_block_size(self):
        return self.__class__.block_size

    def exposed_get_minions(self):
        return self.__class__.minions
        
    def exposed_get_dir_tree(self):
        return self.__class__.dir_tree

    def calc_num_blocks(self,size):
        return int(math.ceil(float(size)/self.__class__.block_size))

    def exists(self,file):
        return file in self.__class__.file_table

    def alloc_blocks(self, dest, num, size):
        blocks = []
        for i in range(0, num):
            block_uuid = uuid.uuid1()
            nodes_ids = random.sample(self.__class__.minions.keys(),self.__class__.replication_factor)
            blocks.append((block_uuid, nodes_ids))

            node = DataNode(metadata={
                                "Size" : size,
                                "Created at":  datetime.datetime.now()
                            },
                            blocks=blocks
            )

        self.create_file_at_path(dest, node)

        return blocks
        
    def node_path(self, node):
        return '/'.join([dir.name for dir in node.path])
    
    def exposed_list(self, path):
        root = self.exposed_get_dir_tree()
        
        dir_node = find(root, lambda node: self.node_path(node) == path)
        
        childrens = ""
        
        if dir_node != None:
            childrens = [child.name for child in dir_node.children]
        
            childrens += dir_node.files.keys()
        else:
            raise NameError("{}".format(path))
        
        return childrens
    
    def exposed_create_file(self, path):
        root = self.exposed_get_dir_tree()
        
        dir_node = find(root, lambda node: self.node_path(node) == path)
        
        childrens = ""
        
        if dir_node != None:
            childrens = [child.name for child in dir_node.children]
        
        childrens += dir_node.files.keys()
        
        return childrens
    
#   get stats about file: size, nodes

    def exposed_file_info(self, fname):
        file_table = self.exposed_get_file_table_entry(fname)
        if not file_table:
            LOG.info("404: file not found")
            return
        
        size = 0
        nodes = set()
        
        for block in file_table:
            for m in [self.exposed_get_minions()[_] for _ in block[1]]:
                data = self.read_from_minion(block[0], m)
                host, port = m
                nodes.add("{} : {}".format(host, port))
                if data:
                    size += sys.getsizeof(data)
                    break
                else:
                    LOG.info("No blocks found. Possibly a corrupt file")
          
        size_in_mb = round(size / 1024 / 1024, 2)
        
        info = "File size: {} MB\n".format(size_in_mb) + "\nNodes adresses:\n" + "\n".join(nodes)
        
        return info
        
    def read_from_minion(master, block_uuid, minion):
        host,port = minion
        con=rpyc.connect(host,port=port)
        minion = con.root.Minion()
        return minion.get(block_uuid)
        
    
    def create_file_at_path(self, path, node):
        root = self.exposed_get_dir_tree()
        
        dir_path = path[:path.rfind('/')]
        
        file_name = path.split('/')[-1]
        
        dir_node = find(root, lambda node: self.node_path(node) == dir_path)
        
        if dir_node != None:
            if hasattr(dir_node, 'files'):         # if dir has files in it
                for key in dir_node.files.keys():
                    if key == file_name:
                        return dir_node.files[key]
                
                dir_node.files[file_name] = node
            else:
                 dir_node.files = defaultdict(list)
                 
                 for key in dir_node.files.keys():
                     if key == file_name:
                         return dir_node.files[key]
                 
                 dir_node.files[file_name] = node
        else:
            raise NameError("Cannot create file: {} {}".format(dir_path, path))
    
    def exposed_dir_exists(self, path):
        return self.get_dir_with_path(path) != None
        
    def get_dir_with_path(self, dir_path):
           root = self.exposed_get_dir_tree()
           
           dir_node = find(root, lambda node: self.node_path(node) == dir_path)
           
           return dir_node
           
    def exposed_make_dir_at_path(self, path):
        root = self.exposed_get_dir_tree()
        
        dir_path = path[:path.rfind('/')]
        
        new_dir_name = path.split('/')[-1]
        
        dir_node = find(root, lambda node: self.node_path(node) == dir_path)
        
        if dir_node != None:
            new_dir = AnyNode(name=new_dir_name, parent=dir_node, files=defaultdict(list))
        else:
            raise NameError("Cannot create dir: {} {}".format(dir_path, path))
    
    def exposed_dir_tree(self):
        root = self.exposed_get_dir_tree()
        
        tree = ""
        
        for pre, fill, node in RenderTree(root):
            tree += "\n%s%s" % (pre, node.name)
            
        return tree + "\n"

if __name__ == "__main__":
    set_conf()
    signal.signal(signal.SIGINT,int_handler)
    t = ThreadedServer(MasterService, port = 2131)
    t.start()
