import datetime
import json
import os
import queue
import socket
import threading
import time

import pandas as pd
from colorama import Fore, Style
from tabulate import tabulate


def tcp():
    thread_list = []
    s = socket.socket()
    s.bind(("0.0.0.0", 30630))
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
            serv_conn, serv_address = service_monitor.accept()
        except socket.timeout:
            pass
        try:
            conn, address = s.accept()
            t_c = threading.Thread(target=connection, args=(conn, address, q))
            thread_list.append(t_c)
            thread_list[len(thread_list) - 1].start()
            n_conn = n_conn + 1
        except socket.timeout:
            pass


def connection(conn, address, q):
    while True:
        try:
            byt = "ok".encode()
            conn.send(byt)
            conn.settimeout(3)
            data = conn.recv(1024).decode('utf-8')
            q.put([data, address])
        except socket.timeout:
            pass
        except ConnectionResetError:
            pass
        except BrokenPipeError:
            pass


def reporter(q):
    d = pd.DataFrame(columns=["address", "name", "timestamp", "status", "latency"])
    while True:
        while not q.empty():
            q_mex = q.get()
            ts, status, lat, address, name = json.loads(q_mex[0])["timestamp"], json.loads(q_mex[0])["status"], \
                                             json.loads(q_mex[0])["latency"], q_mex[1][0], json.loads(q_mex[0])["name"]
            if not any(d.address == address):
                d = d.append(
                    pd.DataFrame([[address, name, ts, status, lat]],
                                 columns=["address", "name", "timestamp", "status", "latency"]))
            else:
                d.loc[d["address"] == address, ["name", "timestamp", "status", "latency"]] = [name, ts, status, lat]
        os.system('cls' if os.name == 'nt' else 'clear')
        print(f"{Fore.YELLOW}ARBOMONITOR [] MURINEDDU CAPITAL, 2021{Style.RESET_ALL}")
        print(f"{Fore.GREEN}\t%d{Style.RESET_ALL}" % (int(datetime.datetime.now(datetime.timezone.utc).timestamp())))
        print(tabulate(d.replace([True, False], ["*", ""]).sort_values("status", ascending=False), headers='keys',
                       tablefmt='psql'))
        my_ts = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
        try:
            if (not d.sort_values("latency")["status"].iloc[0]) and my_ts - int(
                    d.sort_values("latency")["timestamp"].iloc[0]) < 45:
                # STOP QUELLO TRUE, PRESO SORTANDO IL DATAFRAME PER IL TRUE (CHE DOVREBBE ESSERE UNO)
                # AVVIA IL SOCKET PRESO IN CONSIDERAZIONE, OTTENUTO SORTANDO PER LATENCY E PRENDENDO L ADDRESS
                ans = (say(d.sort_values("status", ascending=False)["address"].iloc[0], "STOP"),
                       say(d.sort_values("latency")["address"].iloc[0], "GO"))
                print("ANSW = ", ans)
            if d.sort_values("latency")["status"].iloc[0] and my_ts - int(
                    d.sort_values("latency")["timestamp"].iloc[0]) > 45:
                # PRENDI IL SERVER UP CON LATENZA MIGLIORE E INVIAGLI UN GO
                # AL SERVER IN QUESTIONE NELL IF BISOGNA INVIARE PREVENTIVAMENTE UN SEGNALE DI STOP
                ans = (say(d.sort_values(["status", "latency"], ascending=[True, True])["address"].iloc[0], "GO"),
                       say(d.sort_values("latency")["address"].iloc[0], "STOP"))
                print("ANSW = ", ans)
                # SETTARE BENE I VALORI RESTITUITI DALLA FUNZIONE SAY (MATCH CASE PER OGNI TIPO DI RETURN 0, 1, 2)
        except IndexError:
            pass
        print(d.isin(["*"]).sum().sum())
        if d.isin(["*"]).sum().sum() > 1:
            say(d.replace([True, False], ["*", ""]).sort_values("status", ascending=False)["address"].iloc[1], "STOP")

        time.sleep(5)


def say(address, msg):
    say_socket = socket.socket()
    say_socket.settimeout(5)
    try:
        say_socket.connect((address, 31000))
    except ConnectionRefusedError:
        return 1
    except socket.timeout:
        return 3
    try:
        say_socket.sendall(msg.encode())
        say_socket.close()
    except socket.timeout:
        return 2
    return 0


if __name__ == '__main__':
    tcp()
