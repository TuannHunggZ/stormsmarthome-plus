#!/usr/bin/python3
import os
import time
import sys
import requests
import re
import logging
from prometheus_client import start_http_server, Gauge
from dotenv import load_dotenv

load_dotenv()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("storm_exporter")

stormUiHost = os.getenv("STORM_UI_HOST")
httpPort = int(os.getenv("PORT_EXPOSE"))
refreshRate = int(os.getenv("REFRESH_RATE"))

# TOPOLOGY/SUMMARY METRICS
STORM_TOPOLOGY_UPTIME_SECONDS = Gauge(
    "storm_topology_uptime_seconds",
    "Shows how long the topology is running in seconds",
    ["topology_name", "topology_id"],
)
STORM_TOPOLOGY_TASKS_TOTAL = Gauge(
    "storm_topology_tasks_total",
    "Total number of tasks for this topology",
    ["topology_name", "topology_id"],
)
STORM_TOPOLOGY_WORKERS_TOTAL = Gauge(
    "storm_topology_workers_total",
    "Number of workers used for this topology",
    ["topology_name", "topology_id"],
)
STORM_TOPOLOGY_EXECUTORS_TOTAL = Gauge(
    "storm_topology_executors_total",
    "Number of executors used for this topology",
    ["topology_name", "topology_id"],
)
STORM_TOPOLOGY_REPLICATION_COUNT = Gauge(
    "storm_topology_replication_count",
    "Number of nimbus hosts on which this topology code is replicated",
    ["topology_name", "topology_id"],
)
STORM_TOPOLOGY_REQUESTED_MEM_ON_HEAP = Gauge(
    "storm_topology_requested_mem_on_heap",
    "Requested On-Heap Memory by User (MB)",
    ["topology_name", "topology_id"],
)
STORM_TOPOLOGY_REQUESTED_MEM_OFF_HEAP = Gauge(
    "storm_topology_requested_mem_off_heap",
    "Requested Off-Heap Memory by User (MB)",
    ["topology_name", "topology_id"],
)
STORM_TOPOLOGY_REQUESTED_TOTAL_MEM = Gauge(
    "storm_topology_requested_total_mem",
    "Requested Total Memory by User (MB)",
    ["topology_name", "topology_id"],
)
STORM_TOPOLOGY_REQUESTED_CPU = Gauge(
    "storm_topology_requested_cpu", "Requested CPU by User (%)", ["topology_name", "topology_id"]
)
STORM_TOPOLOGY_ASSIGNED_MEM_ON_HEAP = Gauge(
    "storm_topology_assigned_mem_on_heap",
    "Assigned On-Heap Memory by Scheduler (MB)",
    ["topology_name", "topology_id"],
)
STORM_TOPOLOGY_ASSIGNED_MEM_OFF_HEAP = Gauge(
    "storm_topology_assigned_mem_off_heap",
    "Assigned Off-Heap Memory by Scheduler (MB)",
    ["topology_name", "topology_id"],
)
STORM_TOPOLOGY_ASSIGNED_TOTAL_MEM = Gauge(
    "storm_topology_assigned_total_mem",
    "Assigned Total Memory by Scheduler (MB)",
    ["topology_name", "topology_id"],
)
STORM_TOPOLOGY_ASSIGNED_CPU = Gauge(
    "storm_topology_assigned_cpu", "Assigned CPU by Scheduler (%)", ["topology_name", "topology_id"]
)

# TOPOLOGY/STATS METRICS:
TOPOLOGY_STATS_TRANSFERRED = Gauge(
    "storm_topology_stats_transferred",
    "Number of messages transferred in given window",
    ["topology_name", "topology_id", "window"],
)
TOPOLOGY_STATS_EMITTED = Gauge(
    "storm_topology_stats_emitted",
    "Number of messages emitted in given window",
    ["topology_name", "topology_id", "window"],
)
TOPOLOGY_STATS_COMPLETE_LATENCY = Gauge(
    "storm_topology_stats_complete_latency",
    "Total latency for processing the message",
    ["topology_name", "topology_id", "window"],
)
TOPOLOGY_STATS_ACKED = Gauge(
    "storm_topology_stats_acked",
    "Number of messages acked in given window",
    ["topology_name", "topology_id", "window"],
)
TOPOLOGY_STATS_FAILED = Gauge(
    "storm_topology_stats_failed",
    "Number of messages failed in given window",
    ["topology_name", "topology_id", "window"],
)

# TOPOLOGY/ID SPOUT METRICS:
STORM_TOPOLOGY_SPOUTS_EXECUTORS = Gauge(
    "storm_topology_spouts_executors",
    "Number of executors for the spout",
    ["topology_name", "topology_id", "spout_id"],
)
STORM_TOPOLOGY_SPOUTS_EMITTED = Gauge(
    "storm_topology_spouts_emitted",
    "Number of messages emitted in given window",
    ["topology_name", "topology_id", "spout_id"],
)
STORM_TOPOLOGY_SPOUTS_COMPLETE_LATENCY = Gauge(
    "storm_topology_spouts_complete_latency",
    "Total latency for processing the message",
    ["topology_name", "topology_id", "spout_id"],
)
STORM_TOPOLOGY_SPOUTS_TRANSFERRED = Gauge(
    "storm_topology_spouts_transferred",
    "Total number of messages transferred in given window",
    ["topology_name", "topology_id", "spout_id"],
)
STORM_TOPOLOGY_SPOUTS_TASKS = Gauge(
    "storm_topology_spouts_tasks",
    "Total number of tasks for the spout",
    ["topology_name", "topology_id", "spout_id"],
)
STORM_TOPOLOGY_SPOUTS_ACKED = Gauge(
    "storm_topology_spouts_acked",
    "Number of messages acked",
    ["topology_name", "topology_id", "spout_id"],
)
STORM_TOPOLOGY_SPOUTS_FAILED = Gauge(
    "storm_topology_spouts_failed",
    "Number of messages failed",
    ["topology_name", "topology_id", "spout_id"],
)

# TOPOLOGY/ID BOLT METRICS:
STORM_TOPOLOGY_BOLTS_PROCESS_LATENCY = Gauge(
    "storm_topology_bolts_process_latency",
    "Average time of the bolt to ack a message after it was received",
    ["topology_name", "topology_id", "bolt_id"],
)
STORM_TOPOLOGY_BOLTS_CAPACITY = Gauge(
    "storm_topology_bolts_capacity",
    "This value indicates the number of messages executed * average execute latency / time window",
    ["topology_name", "topology_id", "bolt_id"],
)
STORM_TOPOLOGY_BOLTS_EXECUTE_LATENCY = Gauge(
    "storm_topology_bolts_execute_latency",
    "Average time to run the execute method of the bolt",
    ["topology_name", "topology_id", "bolt_id"],
)
STORM_TOPOLOGY_BOLTS_EXECUTORS = Gauge(
    "storm_topology_bolts_executors",
    "Number of executor tasks in the bolt component",
    ["topology_name", "topology_id", "bolt_id"],
)
STORM_TOPOLOGY_BOLTS_TASKS = Gauge(
    "storm_topology_bolts_tasks",
    "Number of instances of bolt",
    ["topology_name", "topology_id", "bolt_id"],
)
STORM_TOPOLOGY_BOLTS_ACKED = Gauge(
    "storm_topology_bolts_acked",
    "Number of tuples acked by the bolt",
    ["topology_name", "topology_id", "bolt_id"],
)
STORM_TOPOLOGY_BOLTS_FAILED = Gauge(
    "storm_topology_bolts_failed",
    "Number of tuples failed by the bolt",
    ["topology_name", "topology_id", "bolt_id"],
)
STORM_TOPOLOGY_BOLTS_EMITTED = Gauge(
    "storm_topology_bolts_emitted",
    "Number of tuples emitted by the bolt",
    ["topology_name", "topology_id", "bolt_id"],
)

# CLUSTER METRICS
STORM_CLUSTER_WORKERS_TOTAL = Gauge(
    "storm_topology_worker_cluster_total",
    "Number of worker processes total in cluster",
    ["ClusterHost"],
)


STORM_CLUSTER_WORKERS_USED = Gauge(
    "storm_topology_worker_cluster_used",
    "Number of worker processes used in cluster",
    ["ClusterHost"],
)

SUPERVISOR_CLUSTER_TOTAL = Gauge(
    "storm_topology_supervisor_cluster_total",
    "Number of supervisors in cluster",
    ["ClusterHost"],
)

WEIGHT_SCALE = Gauge(
    "storm_topology_weight_scale",
    "Metric used to determine scaling in KEDA based on worker_process_used/worker_process_total, spouts_complete_latency, bolts_capacity",
    ["ClusterHost"],
)



def getMetric(metric):
    if metric is None:
        return 0
    else:
        return metric


def statsMetric(stat, tn, tid):
    wd = stat["window"]
    TOPOLOGY_STATS_TRANSFERRED.labels(tn, tid, wd).set(getMetric(stat["transferred"]))
    TOPOLOGY_STATS_EMITTED.labels(tn, tid, wd).set(getMetric(stat["emitted"]))
    TOPOLOGY_STATS_COMPLETE_LATENCY.labels(tn, tid, wd).set(
        getMetric(stat["completeLatency"])
    )
    TOPOLOGY_STATS_ACKED.labels(tn, tid, wd).set(getMetric(stat["acked"]))
    TOPOLOGY_STATS_FAILED.labels(tn, tid, wd).set(getMetric(stat["failed"]))


def spoutMetric(spout, tn, tid):
    sid = spout["spoutId"]
    STORM_TOPOLOGY_SPOUTS_EXECUTORS.labels(tn, tid, sid).set(
        getMetric(spout["executors"])
    )
    STORM_TOPOLOGY_SPOUTS_EMITTED.labels(tn, tid, sid).set(getMetric(spout["emitted"]))
    STORM_TOPOLOGY_SPOUTS_COMPLETE_LATENCY.labels(tn, tid, sid).set(
        getMetric(spout["completeLatency"])
    )
    STORM_TOPOLOGY_SPOUTS_TRANSFERRED.labels(tn, tid, sid).set(
        getMetric(spout["transferred"])
    )
    STORM_TOPOLOGY_SPOUTS_TASKS.labels(tn, tid, sid).set(getMetric(spout["tasks"]))
    STORM_TOPOLOGY_SPOUTS_ACKED.labels(tn, tid, sid).set(getMetric(spout["acked"]))
    STORM_TOPOLOGY_SPOUTS_FAILED.labels(tn, tid, sid).set(getMetric(spout["failed"]))


def boltMetric(bolt, tn, tid):
    bid = bolt["boltId"]
    STORM_TOPOLOGY_BOLTS_CAPACITY.labels(tn, tid, bid).set(
        getMetric(bolt["capacity"])        
    )
    STORM_TOPOLOGY_BOLTS_PROCESS_LATENCY.labels(tn, tid, bid).set(
        getMetric(bolt["processLatency"])
    )
    STORM_TOPOLOGY_BOLTS_EXECUTE_LATENCY.labels(tn, tid, bid).set(
        getMetric(bolt["executeLatency"])
    )
    STORM_TOPOLOGY_BOLTS_EXECUTORS.labels(tn, tid, bid).set(
        getMetric(bolt["executors"])
    )
    STORM_TOPOLOGY_BOLTS_TASKS.labels(tn, tid, bid).set(getMetric(bolt["tasks"]))
    STORM_TOPOLOGY_BOLTS_ACKED.labels(tn, tid, bid).set(getMetric(bolt["acked"]))
    STORM_TOPOLOGY_BOLTS_FAILED.labels(tn, tid, bid).set(getMetric(bolt["failed"]))
    STORM_TOPOLOGY_BOLTS_EMITTED.labels(tn, tid, bid).set(getMetric(bolt["emitted"]))

def clusterMetric(cluster_metrics, cluster_hostname):
    STORM_CLUSTER_WORKERS_TOTAL.labels(cluster_hostname).set(getMetric(cluster_metrics["slotsTotal"]))
    STORM_CLUSTER_WORKERS_USED.labels(cluster_hostname).set(getMetric(cluster_metrics["slotsUsed"]))
    SUPERVISOR_CLUSTER_TOTAL.labels(cluster_hostname).set(getMetric(cluster_metrics["supervisors"]))


def topologyMetric(topology):
    tn = topology["name"]
    tid = topology["id"]
    for stat in topology["topologyStats"]:
        statsMetric(stat, tn, tid)
    for spout in topology["spouts"]:
        spoutMetric(spout, tn, tid)
    for bolt in topology["bolts"]:
        boltMetric(bolt, tn, tid)


def topologySummaryMetric(topology_summary, stormUiHost):
    tn = topology_summary["name"]
    tid = topology_summary["id"]
    STORM_TOPOLOGY_UPTIME_SECONDS.labels(tn, tid).set(topology_summary["uptimeSeconds"])
    STORM_TOPOLOGY_TASKS_TOTAL.labels(tn, tid).set(topology_summary["tasksTotal"])
    STORM_TOPOLOGY_WORKERS_TOTAL.labels(tn, tid).set(topology_summary["workersTotal"])
    STORM_TOPOLOGY_EXECUTORS_TOTAL.labels(tn, tid).set(
        topology_summary["executorsTotal"]
    )
    STORM_TOPOLOGY_REPLICATION_COUNT.labels(tn, tid).set(
        topology_summary["replicationCount"]
    )
    STORM_TOPOLOGY_REQUESTED_MEM_ON_HEAP.labels(tn, tid).set(
        topology_summary["requestedMemOnHeap"]
    )
    STORM_TOPOLOGY_REQUESTED_MEM_OFF_HEAP.labels(tn, tid).set(
        topology_summary["requestedMemOffHeap"]
    )
    STORM_TOPOLOGY_REQUESTED_TOTAL_MEM.labels(tn, tid).set(
        topology_summary["requestedTotalMem"]
    )
    STORM_TOPOLOGY_REQUESTED_CPU.labels(tn, tid).set(topology_summary["requestedCpu"])
    STORM_TOPOLOGY_ASSIGNED_MEM_ON_HEAP.labels(tn, tid).set(
        topology_summary["assignedMemOnHeap"]
    )
    STORM_TOPOLOGY_ASSIGNED_MEM_OFF_HEAP.labels(tn, tid).set(
        topology_summary["assignedMemOffHeap"]
    )
    STORM_TOPOLOGY_ASSIGNED_TOTAL_MEM.labels(tn, tid).set(
        topology_summary["assignedTotalMem"]
    )
    STORM_TOPOLOGY_ASSIGNED_CPU.labels(tn, tid).set(topology_summary["assignedCpu"])

    try:
        r = requests.get("http://" + stormUiHost + "/api/v1/topology/" + tid+"?window=600")
        topologyMetric(r.json())
    except requests.exceptions.RequestException as e:
        print(e)
        sys.exit(1)

def weight_scale_metric(cluster_metrics, cluster_hostname, topology_summary=None):
    ratio_worker_process = float(getMetric(cluster_metrics["slotsUsed"]))/float(getMetric(cluster_metrics["slotsTotal"]))
    
    tid = topology_summary["id"]
    r = requests.get("http://" + stormUiHost + "/api/v1/topology/" + tid+"?window=600")
    topology_metric = r.json()
    bolt_capacity_values = []
    spout_latency_values = []
    for bolt in topology_metric["bolts"]:
        if re.match(r"^split-.*", bolt["boltId"]):
            bolt_capacity_values.append(float(bolt["capacity"]))
    for spout in topology_metric["spouts"]:
        if spout["spoutId"] == "spout-data-iot-data":
            spout_latency_values.append(float(spout["completeLatency"])) 
    avg_bolt_capacity = sum(bolt_capacity_values) / len(bolt_capacity_values) if bolt_capacity_values else 0
    avg_spout_complete = sum(spout_latency_values) / len(spout_latency_values) if spout_latency_values else 0
    weight_scale_cal = ratio_worker_process * 0.6/0.7 + (avg_bolt_capacity/0.5)*0.2 + (avg_spout_complete/100)*0.2
    WEIGHT_SCALE.labels(cluster_hostname).set(weight_scale_cal)
    return ratio_worker_process

start_http_server(httpPort)
while True:
    try:
        request_cluster_metrics = requests.get(("http://" + stormUiHost + "/api/v1/cluster/summary"))
        clusterMetric(request_cluster_metrics.json(), stormUiHost)

        r = requests.get("http://" + stormUiHost + "/api/v1/topology/summary")
        logger.info("caught metrics from %s",str(r.json()))
        for topology in r.json()["topologies"]:
            topologySummaryMetric(topology, stormUiHost)
            weight_scale_metric(request_cluster_metrics.json(), stormUiHost, topology)
            
    except requests.exceptions.RequestException as e:
        logger.error(e)
        sys.exit(1)
    time.sleep(refreshRate)