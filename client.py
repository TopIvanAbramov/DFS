import rpyc
import sys
import os
import logging
from hurry.filesize import size

from anytree import NodeMixin, RenderTree, AnyNode

CURRENT_DIR = "."

logging.basicConfig(level=logging.DEBUG)
LOG = logging.getLogger(__name__)


def send_to_minion(block_uuid, data, minions):
    LOG.info("sending: " + str(block_uuid) + str(minions))
    minion = minions[0]
    minions = minions[1:]
    host, port = minion

    con = rpyc.connect(host, port=port)
    minion = con.root.Minion()
    minion.put(block_uuid, data, minions)


def read_from_minion(block_uuid, minion):
    host, port = minion
    con = rpyc.connect(host, port=port)
    minion = con.root.Minion()
    return minion.get(block_uuid)


def get(master, fname):
    name, extension = fname.split('.')

    file_table = master.get_file_table_entry(CURRENT_DIR + "/" + fname)
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


def delete_file(master, path):
    master.delete_file(CURRENT_DIR + "/" + path)


def get_new_filename(name, extension):
    try:
        f = open(name + "." + extension)
        f.close()
        i = 1
        while True:  # try different file names
            try:  # untill find one which is free
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


def put(master, source, destination):
    size = os.path.getsize(source)
    blocks = master.write(CURRENT_DIR + "/" + destination, size)
    with open(source, 'rb') as f:
        for b in blocks:
            data = f.read(master.get_block_size())
            block_uuid = b[0]
            minions = [master.get_minions()[_] for _ in b[1]]
            send_to_minion(block_uuid, data, minions)


def file_info(master, fname):
    file_info = master.file_info(CURRENT_DIR + "/" + fname)

    for key in file_info:
        print("{} : {}".format(key, file_info[key]))


def list(master):
    for file in master.list(CURRENT_DIR):
        print(file)


def change_dir(master, new_dir):
    global CURRENT_DIR

    if new_dir == "~":
        CURRENT_DIR = "."
    else:
        if master.dir_exists(CURRENT_DIR + "/" + new_dir):
            CURRENT_DIR = CURRENT_DIR + "/" + new_dir
        else:
            raise NameError("Directory not exists")


def move(master, file_path, new_dir):
    master.move(file_path, new_dir)
    
def make_dir_at_path(master, dir_name):
    if not "/" in dir_name:
        master.make_dir_at_path(CURRENT_DIR + "/" + dir_name)
    else:
        raise NameError("Directory name cannot contain '/'")

def rm(master, dir_name, force=True):
    master.remove_dir(CURRENT_DIR + "/" + dir_name, force)

def dir_tree(master):
    print(master.dir_tree())


def init(master):
    print(size(master.init()))


def clear():
    os.system('cls' if os.name == 'nt' else 'clear')


def main():
    con = rpyc.connect("localhost", port=2131)
    master = con.root.Master()

    while True:
        try:
            args = input(">> ").split()

            if args[0] == "get":
                get(master, args[1])
            elif args[0] == "init":
                init(master)
            elif args[0] == "put":
                put(master, args[1], args[2])
            elif args[0] == "rename" or args[0] == "move":
                rename_move(master, args[1], args[2])
            elif args[0] == "delete":
                delete_file(master, args[1])
            elif args[0] == "ls":
                list(master)
            elif args[0] == "cd":
                change_dir(master, args[1])
            elif args[0] == "info":
                file_info(master, args[1])
            elif args[0] == "mkdir":
                make_dir_at_path(master, args[1])
            elif args[0] == "tree":
                dir_tree(master)
            elif args[0] == "tree":
                move(master, args[1], args[2])
            elif args[0] == "rm":
                if args[1] == "-f":
                    rm(master, args[2], force=True)
                    print("False")
                else:
                    rm(master, args[1], force=False)
            elif args[0] == "clear":
                clear()
            else:
                raise NameError("Unknown command")
        except Exception as e:
            print(e)


if __name__ == "__main__":
    main()
