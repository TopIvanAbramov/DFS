import rpyc
import sys
import os
import logging

CURRENT_DIR = "./"

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)

def send_to_minion(block_uuid,data,minions):
    LOG.info("sending: " + str(block_uuid) + str(minions))
    minion=minions[0]
    minions=minions[1:]
    host,port=minion

    con=rpyc.connect(host,port=port)
    minion = con.root.Minion()
    minion.put(block_uuid,data,minions)


def read_from_minion(block_uuid,minion):
  host,port = minion
  con=rpyc.connect(host,port=port)
  minion = con.root.Minion()
  return minion.get(block_uuid)

def get(master, fname):
    name, extension = fname.split('.')
  
    file_table = master.get_file_table_entry(fname)
    if not file_table:
        LOG.info("404: file not found")
        return

    if not os.path.exists(os.path.dirname(fname)) and "/" in fname:
        os.makedirs(os.path.dirname(fname))
        
    new_filename = get_new_filename(name, extension)
    f = open(new_filename, "wb")
  
    for block in file_table:
        for m in [master.get_minions()[_] for _ in block[1]]:
            data = read_from_minion(block[0], m)
            if data:
                f.write(data)
                break
            else:
                LOG.info("No blocks found. Possibly a corrupt file")
    f.close()
  
#  Change file name/move file

def rename_move(master, old_path, new_path):
    master.change_filepath(old_path, new_path)

def delete_file(master, file_path):
    master.delete_file(file_path)

def get_new_filename(name, extension):
    try:
        f = open(name + "." + extension)
        f.close()
        i = 1
        while True:                                                     # try different file names
            try:                                                        # untill find one which is free
                f = open(name + "_copy{}".format(i) + "." + extension)
                f.close()
                i += 1
                continue
            except IOError:
                return name + "_copy{}".format(i) + "." + extension

    except IOError:
        f = open(name + "." + extension, "w+")
        f.close()
        return name + "." + extension

def put(master, source):
    size = os.path.getsize(source)
    blocks = master.write(source,size)
    with open(source, 'rb') as f:
      for b in blocks:
        data = f.read(master.get_block_size())
        block_uuid=b[0]
        minions = [master.get_minions()[_] for _ in b[1]]
        send_to_minion(block_uuid,data,minions)
        
def file_info(master, fname):
  file_info = master.file_info(fname)
  print(file_info)


def list(master, source):
    for file in master.list(source):
        print(file)
    
def change_dir(new_dir):
    global CURRENT_DIR
    
    if new_dir == "~":
        CURRENT_DIR = "./"
    else:
        CURRENT_DIR = str(new_dir)

def clear():
    os.system('cls' if os.name == 'nt' else 'clear')

def main():
    con = rpyc.connect("localhost", port = 2131)
    master=con.root.Master()
  
    while True:
        try:
            print(">> ", end="")
            args = input().split()
            
            if args[0] == "get":
                get(master,args[1])
            elif args[0] == "put":
                put(master,args[1])
            elif args[0] == "rename" or args[0] == "move":
                rename_move(master, args[1], args[2])
            elif args[0] == "delete":
                delete_file(master, args[1])
            elif args[0] == "ls":
                list(master, CURRENT_DIR)
            elif args[0] == "cd":
                change_dir(args[1])
            elif args[0] == "info":
                file_info(master, args[1])
            elif args[0] == "clear":
                clear()
            else:
                raise NameError("Unknown command")
        except Exception as e:
            print(e)


if __name__ == "__main__":
    main()
