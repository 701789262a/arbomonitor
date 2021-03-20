import datetime
import json
import os
import queue
import socket
import threading
import time
from tabulate import tabulate
import pandas as pd
from colorama import Fore, Style


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
        except socket.timeout:
            pass


def connection(conn, addr, q, d):
    while True:
        try:
            byt = "ok".encode()
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
    d = pd.DataFrame(columns=["address", "timestamp", "status", "latency"])
    while True:
        while not q.empty():
            q_mex = q.get()
            ts, status, lat, addr = json.loads(q_mex[0])["timestamp"], json.loads(q_mex[0])["status"], \
                                    json.loads(q_mex[0])["latency"], q_mex[1][0]
            if not any(d.address == addr):
                d = d.append(
                    pd.DataFrame([[addr, ts, lat, status]], columns=["address", "timestamp", "status", "latency"]))
            else:
                d.loc[d["address"] == addr, ["timestamp", "status", "latency"]] = [ts, status, lat]
        os.system('cls' if os.name == 'nt' else 'clear')
        print(f"{Fore.YELLOW}ARBOMONITOR [] MURINEDDU CAPITAL, 2021{Style.RESET_ALL}")
        print(d)
        print(tabulate(d, headers='keys', tablefmt='psql'))
        my_ts = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
        try:
            if d.sort_values("latency")["status"].iloc[0] == "False" and my_ts - int(
                    d.sort_values("latency")["timestamp"].iloc[0]) < 45:
                print("INVIA RICHIESTA PER DIVENTARE TRADER A %s PER MIGLIORE LATENZA" % (d.sort_values("latency")["address"].iloc[0]))
            if d.sort_values("latency")["status"].iloc[0] == "True" and my_ts - int(
                    d.sort_values("latency")["timestamp"].iloc[0]) > 45:
                print("INVIA RICHIESTA PER DIVENTARE TRADER A %s PER DOWNTIME SERVER MIGLIORE" % (d.sort_values("latency")["address"].iloc[0]))
        except IndexError:
            pass
        time.sleep(1)


if __name__ == '__main__':
    tcp()
