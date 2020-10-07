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

## How to install. Server.

Run `python3 master.py` on your server machine, to run Name Server.

Run `python3 minion.py host_ip port_number` on your server machine, to run Storage Server, `host_ip` is the IP that will be used to contact this minion.

## How to install. Client.

Run `python3 client.py` on your client machine, then proceed to the commands (see below).

## How to use. Client.

We have implemented such client commands in our Distributed File System:
- `init` - initializes the client storage on a new system, removes any existing file in the dfs root directory and returns available size
- `create file_name` - creates a new empty file
- `get file_path` - downloads a file from the DFS to the Client side
- `put local_file_path remote_file_path` - uploads a file from the Client side to the DFS
- `delete file_path` - deletes any file from DFS
- `info file_path` - provides information about the file
- `copy file_path` - creates a copy of file in the same directory
- `move file_path distanation_path` - moves a file to the specified path
- `cd directory_path` - changes directory (use `cd ~` to change to the root directory)
- `ls` - returns list of files, which are stored in the directory
- `mkdir directory_name` - creates a new directory
- `rm directory_path` - deletes directory, if the directory contains files asks for confirmation from the user before deletion

## Architectural diagram.

![Architectural diagram](https://github.com/TopIvanAbramov/DFS/blob/master_hbq/DFS-3.png)

## Description of communication protocol.

We have two main communication paths, between Client and Name Server and between Name Server and Storage Server. All our communication uses remote procedure call (RPC) as a communication protocol. So, it occurs when our program causes a procedure to execute on another machine on a shared network, which is coded as if it were a normal (local) procedure call, without explicitly coding the details for the remote interaction. We have written essentially the same code whether the subroutine is local to the executing program or remote. This is a form of client-server interaction (caller is client, executor is server), typically implemented via a request-response message-passing system. In the object-oriented programming paradigm, RPCs are represented by remote method invocation (RMI). The RPC model implies a level of location transparency, namely that calling procedures are largely the same whether they are local or remote, but usually, they are not identical, so local calls can be distinguished from remote calls. Remote calls are usually orders of magnitude slower and less reliable than local calls, so distinguishing them is important. RPCs are a form of inter-process communication, in that different processes have different address spaces: if on the same host machine, they have distinct virtual address spaces, even though the physical address space is the same; while if they are on different hosts, the physical address space is different.

RPC is a request-response protocol. An RPC is initiated by the client, which sends a request message to a known remote server to execute a specified procedure with supplied parameters. The remote server sends a response to the client, and the application continues its process. While the server is processing the call, the client is blocked (it waits until the server has finished processing before resuming execution), unless the client sends an asynchronous request to the server.

## Provable contribution of each team member.

Firstly, we started with reading all useful material, like how Distributed File Systems work, which approaches could be implemented and which problems might occur. Next, we have a discussion about implementation language, all of us decided to choose Python as an implementation language. After that, we proceed with designing the architecture for our Distributed File System. For this task, we had zoom meetings with discussion of our project and its structure. Then, we have distributed coding tasks between each other: Zarubin Iurii was writing code for the Name Server part, Abramov Ivan was coding Client part and Plevako Matvey was busy with Minions part. Afterwards, Zarubin Iurii has started instances on AWS for testing our Distributed File System and Plevako Matvey and Abramov Ivan proceed with testing it on AWS. Later Plevako Matvey and Abramov Ivan started code debugging and Zarubin Iurii started writing the report and documentation for our Distributed File System and advising UI/UX-design for our client CLI in Distributed File System. At the end, all of us have reviewed the code and checked it work locally on our machines and on the cloud using AWS instances with VPS. Then Plevako Matvey has created Docker images and upload them into DockerHub. During development, we have several zoom meetings to understand and distribute our tasks. For debugging we have used GitHub issues, so we have not left any problem unsolved.

## GitHub link:

https://github.com/TopIvanAbramov/DFS

## DockerHub link:

Link
