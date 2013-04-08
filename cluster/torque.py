import logging
import os
import re

from lib.util import Command


LOG = logging.getLogger(__name__)      


class Node(object):
    def __init__(self, public_dns_name, np, state):
        self.public_dns_name = public_dns_name
        self.np = np
        self.state = state
        self.terminate_me = False
        
    def __repr__(self):
        return "Node<%s, %s, %s>" % (self.public_dns_name, self.np, self.state)
       

class BaseCluster(object):
    def __init__(self):
        self.num_queued_jobs = 0
        self.num_total_jobs = 0
        self.num_queued_cores = 0
        self.num_total_nodes = 0
        self.num_total_cores = 0
        self.num_free_cores = 0
        self.num_down_cores = 0
        self.nodes = []
        self._public_dns_names = []
        self._has_booted = []


class TorqueCluster(BaseCluster):
    def __init__(self, directory):
        super(TorqueCluster, self).__init__()
        self.directory = directory
        self._qstat_cmd = os.path.join(self.directory, "bin/qstat -a")
        self._pbsnodes_cmd = os.path.join(self.directory, "bin/pbsnodes")
        self._qmgr_cmd = os.path.join(self.directory, "bin/qmgr")
        LOG.debug("Set qstat command: %s" % self._qstat_cmd)
        LOG.debug("Set pbsnodes command: %s" % self._pbsnodes_cmd)
        LOG.debug("Set qmgr command: %s" % self._qmgr_cmd)
        
    def _update_job_info(self):
        qstat = Command([self._qstat_cmd])
        qstat_rc = qstat.execute()
        if qstat_rc != 0:
            LOG.error("qstat returned %d" % qstat_rc)
            return
        job_line = "(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+"
        job_line += "(\d+)\s+(\d+)\s+(\S+)\s+(\S+)\s+([A-Z])\s+(\S+)"
        job_pattern = re.compile(job_line)
        queued_cores = 0
        queued_jobs = 0
        total_jobs = 0
        for line in qstat.stdout.split('\n'):
            match = job_pattern.match(line)
            if match:
                if match.group(10) == 'Q':
                    queued_cores += int(match.group(7))
                    queued_jobs += 1
                total_jobs += 1    
        self.num_queued_jobs = queued_jobs
        self.num_queued_cores = queued_cores
        self.num_total_jobs = total_jobs
        LOG.debug("Jobs updated: %s total jobs and %s queued cores." % (
            self.num_total_jobs, self.num_queued_cores))

    def _update_node_info(self):
        self.nodes = []
        self.num_total_nodes = 0
        self.num_total_cores = 0
        self.num_free_cores = 0
        self.num_down_cores = 0
        pbsnodes_cmd = str(self._pbsnodes_cmd) + " -a"
        pbsnodes = Command([pbsnodes_cmd])
        pbsnodes_rc = pbsnodes.execute()
        if pbsnodes_rc != 0:
            LOG.error("pbsnodes returned %d" % pbsnodes_rc)
            return                                                
        node_line = "\n(\S+)\n\s+state\s=\s(\S+)\n\s+np\s=\s(\d+)\n"
        node_pattern = re.compile(node_line)
        matches = re.findall(node_pattern, pbsnodes.stdout)
        for match in matches:
            n = Node(match[0], int(match[2]), match[1])
            self.num_total_nodes += 1
            self.num_total_cores += int(match[2])
            if match[1] == "free":
                self.num_free_cores += int(match[2])
            if "down" in match[1]:
                self.num_down_cores += int(match[2])
            if "down" not in match[1]:
                if n.public_dns_name not in self._has_booted:
                    self._has_booted.append(n.public_dns_name)
            self.nodes.append(n)
        LOG.debug("Nodes updated: %s total nodes and %s total cores." % (
            self.num_total_nodes, self.num_total_cores))

    def _update_public_dns_names(self):
        self._public_dns_names = [x.public_dns_name for x in self.nodes]
        
    def _add_new_node(self, public_dns_name, np):
        qmgr_cmd = str(self._qmgr_cmd) + " -c \"create node %s np=%d\""
        qmgr_cmd = qmgr_cmd % (public_dns_name, np)
        add_node = Command([qmgr_cmd])
        add_node_rc = add_node.execute()
        if add_node_rc != 0:
            LOG.error("qmgr returned %d" % add_node_rc)
            return
        LOG.debug("Successfully added node: %s" % public_dns_name)

    def _remove_node(self, public_dns_name):
        qmgr_cmd = str(self._qmgr_cmd) + " -c \"delete node %s\""
        qmgr_cmd = qmgr_cmd % public_dns_name
        remove_node = Command([qmgr_cmd])
        remove_node_rc = remove_node.execute()
        if remove_node_rc != 0:
            LOG.error("qmgr returned %d" % remove_node_rc)
            return
        if public_dns_name in self._has_booted:
            self._has_booted.remove(public_dns_name)
        LOG.debug("Successfully removed node: %s" % public_dns_name)
    
    def remove_node(self, public_dns_name):
        if public_dns_name in self._public_dns_names:
            LOG.debug("%s is in the cluster, removing" % public_dns_name)
            self._remove_node(public_dns_name)
        else:
            LOG.debug("%s is not in the cluster, cannot remove" % (
                public_dns_name))

    def add_node(self, public_dns_name, np=1):
        if not (public_dns_name in self._public_dns_names):
            LOG.debug("Adding node to cluster: %s" % public_dns_name)
            self._add_new_node(public_dns_name, np)

    def offline_node(self, public_dns_name):
        pbsnodes_cmd = str(self._pbsnodes_cmd) + " -o %s"
        pbsnodes_cmd = pbsnodes_cmd % public_dns_name
        offline_node = Command([pbsnodes_cmd])
        offline_node_rc = offline_node.execute()
        if offline_node_rc != 0:
            LOG.error("pbsnodes returned %d" % offline_node_rc)
            return
        else:
            LOG.debug("Successfully marked node offline: %s" % public_dns_name)
            for node in self.nodes:
                if node.public_dns_name == public_dns_name:
                    node.terminate_me = True

    def update(self):
        LOG.debug("Updating cluster nodes and job information.")
        self._update_job_info()
        self._update_node_info()
        self._update_public_dns_names()
        LOG.debug("Nodes successfully booted: %s" % self._has_booted)
        
    def get_num_queued_jobs(self):
        return self.num_queued_jobs

    def get_num_queued_job_cores(self):
        return self.num_queued_cores
        
    def get_num_total_jobs(self):
        return self.num_total_jobs

    def get_num_down_cluster_cores(self):
        return self.num_down_cores
        
    def get_num_free_cluster_cores(self):
        return self.num_free_cores
        
    def get_num_total_cluster_cores(self):
        return self.num_total_cores
    
    def get_num_total_cluster_nodes(self):
        return self.num_total_nodes
    
    def get_public_dns_names_of_idle_or_down_nodes(self, require_booted=False):
        names = []
        for node in self.nodes:
            if (((("idle" in node.state) or ("down" in node.state) or 
                ("offline" in node.state) or ("free" in node.state))) and 
                (not "job-exclusive" in node.state)):
                if require_booted:
                    if node.public_dns_name in self._has_booted:
                        names.append(node.public_dns_name)
                else:
                    names.append(node.public_dns_name)
        LOG.debug("Public DNS names of idle and down nodes: %s" % names)
        return names