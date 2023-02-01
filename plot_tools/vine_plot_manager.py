from collections import Counter
import matplotlib.pyplot as plt
import time
import sys
import argparse

manager_info = {"Start":-1,
                "Connections":[],
                "Disconnections":[],
                "Resource Updates":[],
                "Cache Updates":[],
                "Tasks Sent":[],
                "Notified Results":[],
                "Marked Done":[],
                "Input Transfers":[],
                "Output Transfers":[],}

def read_log(log, print_stats=False):
    filename = log 
    lines = open(log, 'r').read().splitlines()
    for line in lines:
        if "MANAER START" in line and "#" not in line:
            sp = line.split()
            time = int(sp[0])/1000000
            manager_info["Start"] = time
        if "WORKER" in line and "CONNECTION" in line and "#" not in line and "DISCONNECTION" not in line:
            sp = line.split()
            time = int(sp[0])/1000000 - manager_info["Start"]
            manager_info["Connections"].append(time)
        elif "WORKER" in line and "DISCONNECTION" in line and "#" not in line:
            sp = line.split()
            time = int(sp[0])/1000000 - manager_info["Start"]
            manager_info["Disconnections"].append(time)
        elif "WORKER" in line and "RESOURCES" in line and "#" not in line:
            sp = line.split()
            time = int(sp[0])/1000000 - manager_info["Start"]
            manager_info["Resource Updates"].append(time)
        elif "CACHE-UPDATE" in line and "#" not in line:
            sp = line.split()
            time = int(sp[0])/1000000 - manager_info["Start"]
            manager_info["Cache Updates"].append(time)
        elif "TASK" in line and "RUNNING" in line and "#" not in line:
            sp = line.split()
            time = int(sp[0])/1000000 - manager_info["Start"]
            manager_info["Tasks Sent"].append(time)
        elif "TASK" in line and "WAITING_RETRIEVAL" in line and "#" not in line:
            sp = line.split()
            time = int(sp[0])/1000000 - manager_info["Start"]
            manager_info["Notified Results"].append(time)
        elif "TASK" in line and "DONE SUCCESS" in line and "#" not in line:
            sp = line.split()
            time = int(sp[0])/1000000 - manager_info["Start"]
            manager_info["Marked Done"].append(time)
        elif "TRANSFER" in line and "INPUT" in line and "#" not in line:
            sp = line.split()
            time = int(sp[0])/1000000 - manager_info["Start"]
            manager_info["Input Transfers"].append(time)
        elif "TRANSFER" in line and "OUTPUT" in line and "#" not in line:
            sp = line.split()
            time = int(sp[0])/1000000 - manager_info["Start"]
            manager_info["Output Transfers"].append(time)

def plot_manager(title='Manager Info', all_info=False, save=None, connections=False, disconnections=False,
                resources=False, cache=False, sent=False, results=False, done=False, IT=False, OT=False):
    
    for category in manager_info:
        if category == "Start":
            continue
        if not connections and not all_info and category == "Connections":
            continue
        if not disconnections and not all_info and category == "Disconnections":
            continue
        if not resources and not all_info and category == "Resource Updates":
            continue
        if not cache and not all_info and category == "Cache Updates":
            continue
        if not sent and not all_info and category == "Tasks Sent":
            continue
        if not results and not all_info and category == "Notified Results":
            continue
        if not done and not all_info and category == "Marked Done":
            continue
        if not IT and not all_info and category == "Input Transfers":
            continue
        if not OT and not all_info and category == "Output Transfers":
            continue
        count = 1
        total = len(manager_info[category])
        xs = []
        ys = []
        for data in manager_info[category]:
            xs.append(data)
            ys.append(count/total)
            count += 1
        plt.plot(xs, ys, label=category)

    plt.title(title)    
    plt.ylabel("Percent of each category of Tasks Done")
    plt.xlabel("time")
    plt.legend()
    plt.show()
    if save:
        plt.savefig(save)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Plot worker information from transaction log file.')
    parser.add_argument('log', help='Path to transaction log file')
    parser.add_argument('title', nargs='?', default ='Manager Info', help='Title of the plot')
    parser.add_argument('save', nargs='?', default = None, help='Save Figure')
    parser.add_argument('-a', action='store_true', help='display all from the transaction log')
    parser.add_argument('-c', action='store_true', help='display connections')
    parser.add_argument('-d', action='store_true', help='display disconnections')
    parser.add_argument('-r', action='store_true', help='display resource updates')
    parser.add_argument('-u', action='store_true', help='display cache updates')
    parser.add_argument('-s', action='store_true', help='display sending tasks')
    parser.add_argument('-w', action='store_true', help='display notified results')
    parser.add_argument('-m', action='store_true', help='display marked done')
    parser.add_argument('-i', action='store_true', help='display input transfers')
    parser.add_argument('-o', action='store_true', help='display output transfes')
    args = parser.parse_args()
    read_log(args.log)
    plot_manager(title=args.title, 
                 save=args.save, 
                 all_info=args.a, 
                 connections = args.c,
                 disconnections = args.d,
                 resources=args.r,
                 cache=args.u,
                 sent=args.s,
                 results=args.w,
                 done=args.m,
                 IT=args.i,
                 OT=args.o) 
