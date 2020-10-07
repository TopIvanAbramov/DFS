# Distributed Systems.
## Project 2: Distributed File System.

### BS18-SE-01
Iurii Zarubin

Ivan Abramov
### BS18-SE-02
Matvey Plevako

#### 07.10.2020

## Introduction

The Distributed File System (DFS) is a file system with data stored on a server. The data is accessed and processed as if it was stored on the local client machine.  The DFS makes it convenient to share information and files among users on a network. We have implemented a simple Distributed File System (DFS). Files are hosted remotely on storage servers. A single naming server indexes the files, indicating which one is stored where. When a client wishes to access a file, it first contacts the naming server to obtain information about the storage server hosting it. After that, it communicates directly with the storage server to complete the operation.

Our file system supports file reading, writing, creation, deletion, copy, moving and info queries. It also supports certain directory operations - listing, creation, changing and deletion. Files are replicated on multiple storage servers. DFS is fault-tolerant and the data is accessible even if some of the network nodes are offline.

We have chosen Python language for Distributed File System implementation.

## Hot to install. Server.

Run `python3 master.py` on your server machine, to run Name Server

Run `python3 minion.py port_number` on your server machine, to run Storage Server

## Hot to install. Client.

Run `python3 client.py` on your client machine, then proceed to the commands (see below).

## Hot to use. Client.

We have implemented such client commands in our Distributed File System:
- `init` - initializes the client storage on a new system, removes any existing file in the dfs root directory and returns available size
- `create file_name` - creates a new empty file
- `get file_path` - downloads a file from the DFS to the Client side
- `put local_file_path` - uploads a file from the Client side to the DFS
- `delete file_path` - deletes any file from DFS
- `info file_path` - provides information about the file
- `copy file_path` - creates a copy of file in the same directory
- `rename file_path new_file_name` - renames a file to the specified name
- `move file_path distanation_path` - moves a file to the specified path
- `cd directory_path` - changes directory
- `ls` - returns list of files, which are stored in the directory
- `mkdir directory_name` - creates a new directory
- `rm directory_path` - deletes directory, if the directory contains files asks for confirmation from the user before deletion
