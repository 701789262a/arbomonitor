import json
import queue
import socket
import threading
import time
from multiprocessing.pool import ThreadPool

import pandas as pd
from colorama import Fore, Style

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
    d = dict()
    n_conn = 0
    t_m = threading.Thread(target=reporter, args=(q,))
    t_m.start()
    while True:
        try:
            serv_conn, serv_addr = service_monitor.accept()
        except socket.timeout:
            pass
        try:
            conn, address = s.accept()
            print("new connection at" + str(address))
            t_c = threading.Thread(target=connection, args=(conn, address, q, d))
            thread_list.append(t_c)
            thread_list[len(thread_list) - 1].start()
            n_conn = n_conn + 1
            # os.system('cls' if os.name == 'nt' else 'clear')
        except socket.timeout:
            # os.system('cls' if os.name == 'nt' else 'clear')
            pass


def connection(conn, addr, q, d):
    while True:
        try:
            byt = "ok".encode()
            # print(conn_list[0])
            conn.send(byt)
            conn.settimeout(3)
            data = conn.recv(1024).decode('utf-8')
            q.put([data, addr])
        except socket.timeout:
            pass
        except ConnectionResetError:
            pass
        except BrokenPipeError:
            pass


def reporter(q):
    first_time = True
    d = pd.DataFrame(columns=["address", "timestamp", "status", "latency"])
    while True:
        while not q.empty():
            if first_time:
                print(f"{Fore.YELLOW}ARBOMONITOR [] MURINEDDU CAPITAL, 2021{Style.RESET_ALL}")
            first_time = False
            q_mex = q.get()
            ts, status, lat, addr = json.loads(q_mex[0])["timestamp"], json.loads(q_mex[0])["status"], \
                                    json.loads(q_mex[0])["latency"], q_mex[1][0]
            print("addr", addr, "status", status)
            d = d.append(pd.DataFrame([[addr, ts, lat, status]], columns=["address", "timestamp", "status", "latency"]))
            msg = str(json.loads(q_mex[0])) + str(q_mex[1])
            #d[q_mex[1][0]] = str(json.loads(q_mex[0]))
            # print("prova\t", msg)
        print(d)
        first_time = True
        time.sleep(2)


if __name__ == '__main__':
    tcp()
