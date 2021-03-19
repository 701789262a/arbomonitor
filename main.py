import time
import datetime
import json
import os
import queue
import socket
from colorama import Fore, Style
import threading
from multiprocessing.pool import ThreadPool

pool = ThreadPool()


def tcp():
    port = 30630
    thread_list = []
    s = socket.socket()
    s.bind(("0.0.0.0", port))
    s.settimeout(0.01)
    s.listen(5)
    service_monitor = socket.socket()
    service_monitor.bind(("0.0.0.0", 29590))
    service_monitor.listen(5)
    service_monitor.settimeout(0.01)
    q = queue.Queue()
    n_conn = 0
    t_m = threading.Thread(target=reporter,args=(q,))
    t_m.start()
    while True:
        try:
            serv_conn, serv_addr = service_monitor.accept()
        except socket.timeout:
            pass
        try:
            conn, address = s.accept()
            print("new connection at"+str(address))
            t_c = threading.Thread(target=connection, args=(conn, address, q))
            thread_list.append(t_c)
            thread_list[len(thread_list) - 1].start()
            n_conn = n_conn + 1
            #os.system('cls' if os.name == 'nt' else 'clear')
        except socket.timeout:
            #os.system('cls' if os.name == 'nt' else 'clear')
            pass


def connection(conn, addr, q):
    while True:
        try:
            byt = "ok".encode()
            # print(conn_list[0])
            conn.send(byt)
            conn.settimeout(3)
            data = conn.recv(1024).decode('utf-8')
        except socket.timeout:
            data = "{}"
            pass
        except ConnectionResetError:
            data = "{}"
            pass
        except BrokenPipeError:
            data = "{}"
            pass
        q.put([data,addr])


def reporter(q):
    while True:
        while not q.empty():
            print(f"{Fore.YELLOW}ARBOMONITOR [] MURINEDDU CAPITAL, 2021{Style.RESET_ALL}")
            msg = json.loads(q.get())
            print(msg)
        time.sleep(1)


if __name__ == '__main__':
    tcp()
