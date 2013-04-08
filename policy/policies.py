import datetime
import logging
import math


LOG = logging.getLogger(__name__)


class BasePolicy(object):
    def __init__(self):
        LOG.debug("Loaded %s policy" % self.__class__.__name__)
        
    def execute(self, cluster, clouds):
        LOG.debug("Executing %s policy" % self.__class__.__name__)

    def _launch_instances(self, clouds, num_cores_to_launch=0):
        cloud = clouds.get_cheapest_valid_cloud()
        if cloud:
            num_valid_instances = len(cloud.get_valid_instances())
            if (cloud._asg.desired_capacity > num_valid_instances):
                if cloud.failed_count >= 3:
                    LOG.debug("%s has failed" % cloud.config.name)
                    cloud.failed_launch = True
                    cloud.failed_count = 0
                    cloud.failed_last_valid_count = 0
                    LOG.debug("Resetting capacity to %s for %s" % (
                        num_valid_instances, cloud.config.name))
                    cloud.set_capacity(num_valid_instances)
                elif num_valid_instances != cloud.failed_last_valid_count:
                    LOG.debug("%s appears to still be launching" % (
                        cloud.config.name))
                    cloud.failed_last_valid_count = num_valid_instances
                else:
                    LOG.debug("%s appears stalled" % cloud.config.name)
                    cloud.failed_count += 1
                    LOG.debug("%s failed count: %s" % (cloud.config.name,
                        cloud.failed_count))
            else:
                multiplier = clouds._global_config.getint("Policy", "multiplier")
                cores_per_instance = cloud.config.instance_cores
                num_i = int(math.ceil(num_cores_to_launch / 
                                      float(cores_per_instance)))
                LOG.debug("%s: calculated %d instances to launch" % (
                    self.__class__.__name__, num_i))
                num_i *= multiplier
                LOG.debug("%s: launching %d instances" % (
                    self.__class__.__name__, num_i))
                cloud.launch_autoscale_instances(num_i)
        else:
            LOG.error("No valid clouds remaining, cannot launch instances")
     
    def _mark_nodes_offline(self, cluster, clouds):
        instances_to_charge = []
        for cloud in clouds.get_clouds_low_to_high():
            instances = cloud.get_public_dns_names_close_to_charge()
            instances_to_charge += instances
        unused_nodes = cluster.get_public_dns_names_of_idle_or_down_nodes(
            require_booted=True)
        offline_nodes = set(instances_to_charge) & set(unused_nodes)
        LOG.debug("Marking nodes offline: %s" % offline_nodes)
        for public_dns_name in offline_nodes:
            cluster.offline_node(public_dns_name)
    
    def _terminate_nodes(self, cluster, clouds):
        to_terminate = []
        for node in cluster.nodes:
            if node.terminate_me:
                to_terminate.append(node.public_dns_name)
        LOG.debug("%s: nodes to terminate: %s" % (self.__class__.__name__,
            to_terminate))
        for cloud in clouds.get_clouds_low_to_high():
            ids = cloud.get_instance_ids_for_public_dns_names(to_terminate)
            LOG.debug("%s: ids of idle nodes: %s" % (self.__class__.__name__,
                ids))
            if ids:
                cloud.failed_launch = False
                cloud.failed_count = 0
                cloud.failed_last_valid_count = 0
            cloud.delete_instances(ids)
        LOG.debug("%s: removing nodes from cluster" % self.__class__.__name__)
        for public_dns_name in to_terminate:
            cluster.remove_node(public_dns_name)
    
    def _terminate_idle_instances_before_charge(self, cluster, clouds):
        LOG.debug("%s: terminating idle instances" % self.__class__.__name__)
        self._mark_nodes_offline(cluster, clouds)
        self._terminate_nodes(cluster, clouds)

        
class OnDemand(BasePolicy):
    def __init__(self):
        super(OnDemand, self).__init__()
        
    def execute(self, cluster, clouds):
        super(OnDemand, self).execute(cluster, clouds)        
        

class OnDemandPlusPlus(BasePolicy):
    def __init__(self):
        super(OnDemandPlusPlus, self).__init__()
    
    def execute(self, cluster, clouds):
        super(OnDemandPlusPlus, self).execute(cluster, clouds)
        num_valid_cloud_cores = clouds.get_total_num_valid_cores()
        num_queued_job_cores = cluster.get_num_queued_job_cores()
        num_free_cluster_cores = cluster.get_num_free_cluster_cores()
        num_down_cluster_cores = cluster.get_num_down_cluster_cores()
        num_total_cluster_cores = cluster.get_num_total_cluster_cores()
        num_pending_cores = num_valid_cloud_cores - num_total_cluster_cores
        if num_pending_cores < 0:
            num_pending_cores = 0

        LOG.debug("%s: num_valid_cloud_cores: %d" % (self.__class__.__name__,
                                                     num_valid_cloud_cores))
        LOG.debug("%s: num_queued_job_cores: %d" % (self.__class__.__name__,
                                                    num_queued_job_cores))
        LOG.debug("%s: num_free_cluster_cores: %d" % (self.__class__.__name__,
                                                      num_free_cluster_cores))
        LOG.debug("%s: num_down_cluster_cores: %d" % (self.__class__.__name__,
                                                      num_down_cluster_cores))
        LOG.debug("%s: num_total_cluster_cores: %d" % (self.__class__.__name__,
                                                       num_total_cluster_cores))
        LOG.debug("%s: num_pending_cores: %d" % (self.__class__.__name__,
                                                 num_pending_cores))
        
        num_cores_to_launch = 0
        if num_queued_job_cores > 0:
            num_cores_to_launch = (num_queued_job_cores - 
                                   (num_free_cluster_cores + num_pending_cores +
                                   num_down_cluster_cores))

        LOG.debug("%s: num_cores_to_launch: %d" % (self.__class__.__name__,
                                                   num_cores_to_launch))

        if num_cores_to_launch > 0:
            self._launch_instances(clouds, num_cores_to_launch)
        else:
            self._terminate_idle_instances_before_charge(cluster, clouds)