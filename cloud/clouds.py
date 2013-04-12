import boto
import datetime
import json
import logging
import os
import time

from boto.ec2.autoscale import AutoScaleConnection
from boto.ec2.autoscale import Tag  # needed for Phantom
from boto.ec2.autoscale.group import AutoScalingGroup
from boto.ec2.autoscale.launchconfig import LaunchConfiguration
from boto.regioninfo import RegionInfo
from lib.config import CloudConfig
from lib.config import VALID_RUN_STATES
from lib.util import Command
from lib.util import read_file
from lib.util import write_file


# supress most boto logging
logging.getLogger('boto').setLevel(logging.CRITICAL)
LOG = logging.getLogger(__name__)


class Cloud(object):
    def __init__(self, cloud_config):
        self.config = cloud_config
        self.all_instances = []
        self.failed_launch = False
        self.failed_count = 0
        self.failed_last_valid_count = 0
        self._conn = None
        self._as_conn = None
        self._lc = None
        self._asg = None
        self._last_asg_launch_attempt = None
        self.maxed = False
        self._last_launch_attempt = datetime.datetime.utcnow()
        self._initialize()

    def _create_connection(self):
        LOG.debug("Creating connection for %s" % self.config.name)
        self._conn = boto.connect_ec2(self.config.access_id,
                                      self.config.secret_key)
        self._conn.host = self.config.cloud_uri
        self._conn.port = self.config.cloud_port

    def _create_autoscale_connection(self):
        LOG.debug("Creating autoscale connection for %s" % self.config.name)
        region = RegionInfo(name=self.config.cloud_type,
                            endpoint=self.config.as_uri)
        self._as_conn = AutoScaleConnection(
            aws_access_key_id=self.config.access_id,
            aws_secret_access_key=self.config.secret_key,
            is_secure=True,
            port=self.config.as_port,
            region=region)

    def _create_or_set_launch_configuration(self):
        name = self.config.lc_name
        if not self._lc:
            LOG.debug("Attempting to load launch configuration: %s" % (name))
            lc = self._as_conn.get_all_launch_configurations(names=[name])
            if len(lc) == 1:
                LOG.debug("Launch configuration %s found." % (name))
                self._lc = lc[0]
        if not self._lc:
            #TODO(pdmars): key and security groups are hardcoded for now, gross
            user_data = "phorque_instance"
            LOG.debug("Creating launch configuration %s" % name)
            LOG.debug("\tname: %s" % name)
            LOG.debug("\timage_id: %s" % self.config.image_id)
            LOG.debug("\tinstance_type: %s" % self.config.instance_type)
            LOG.debug("\tuser_data: %s" % user_data)
            self._lc = LaunchConfiguration(
                name=name,
                image_id=self.config.image_id,
                key_name="phantomkey",
                security_groups=['default'],
                instance_type=self.config.instance_type,
                user_data=user_data)
            self._as_conn.create_launch_configuration(self._lc)

    def _create_or_set_autoscale_group(self):
        name = self.config.asg_name
        if not self._asg:
            LOG.debug("Attempting to load autoscale group: %s" % name)
            asg = self._as_conn.get_all_groups(names=[name])
            LOG.debug("Autoscale group: %s" % asg)
            if len(asg) == 1:
                LOG.debug("Autoscale group %s found." % name)
                self._asg = asg[0]
        if not self._asg:
            # TODO(pdmars): more hard coded grossness, for now
            try:
                cloud_guess = self.config.lc_name.split("@")[1].strip()
            except Exception as e:
                LOG.warn("Unable to guess cloud for auto scale tags")
                LOG.warn("Setting cloud to hotel")
                cloud_guess = "hotel"
            policy_name_key = "PHANTOM_DEFINITION"
            policy_name = "error_overflow_n_preserving"
            ordered_clouds_key = "clouds"
            n_preserve_key = "minimum_vms"
            ordered_clouds = cloud_guess + ":-1"
            n_preserve = 0
            policy_tag = Tag(connection=self._as_conn, key=policy_name_key,
                             value=policy_name, resource_id=name)
            clouds_tag = Tag(connection=self._as_conn, key=ordered_clouds_key,
                             value=ordered_clouds, resource_id=name)
            npreserve_tag = Tag(connection=self._as_conn, key=n_preserve_key,
                                value=n_preserve, resource_id=name)
            tags = [policy_tag, clouds_tag, npreserve_tag]
            zones = [self.config.az]
            LOG.debug("Creating autoscale group %s" % name)
            LOG.debug("\tname: %s" % name)
            LOG.debug("\tavailability_zones: %s" % zones)
            LOG.debug("\tlaunch_config: %s" % self._lc)
            self._asg = AutoScalingGroup(group_name=name,
                                         availability_zones=zones,
                                         min_size=0,
                                         max_size=0,
                                         launch_config=self._lc,
                                         tags=tags)
            self._as_conn.create_auto_scaling_group(self._asg)

    def _initialize(self):
        LOG.debug("Initializing %s" % self.config.name)
        self._create_connection()
        self._create_autoscale_connection()
        self._create_or_set_launch_configuration()
        self._create_or_set_autoscale_group()
        LOG.debug("Initialization complete for %s" % self.config.name)

    def get_valid_instances(self):
        return self.all_instances

    def _refresh_instances(self):
        LOG.debug("%s: getting instance information" % self.config.name)
        self.all_instances = []
        instances = []
        as_instances = self._as_conn.get_all_autoscaling_instances()
        as_instance_ids = [i.instance_id for i in as_instances]
        reservations = self._conn.get_all_instances()
        for reservation in reservations:
            for instance in reservation.instances:
                if instance.id in as_instance_ids:
                    if instance.state in VALID_RUN_STATES:
                        instances.append(instance)
        for instance in instances:
            self.all_instances.append(instance)
        num_instances = len(self.all_instances)
        LOG.debug("%s: updated %d instances" % (self.config.name,
                                                num_instances))
        if num_instances >= self.config.max_instances:
            LOG.warn("%s reached the max (%s) instances: %s" % (
                self.config.name, self.config.max_instances,
                num_instances))
            self.maxed = True
        else:
            self.maxed = False

    def _refresh_asg(self):
        LOG.debug("%s: refreshing autoscale group" % self.config.name)
        asg_name = self.config.asg_name
        asgs = self._as_conn.get_all_groups(names=[asg_name])
        if len(asgs) == 1:
            self._asg = asgs[0]
            LOG.debug("\trefreshed autoscale group: %s" % asg_name)
        else:
            LOG.warn("\tunable to refresh autoscale group: %s" % asg_name)

    def refresh(self, cluster):
        self._refresh_instances()
        self._refresh_asg()

    def get_total_num_valid_cores(self):
        LOG.debug("%s: getting number of valid cores" % self.config.name)
        total_num_valid_cores = 0
        num_valid_instances = len(self.get_valid_instances())
        total_valid_cores = num_valid_instances * self.config.instance_cores
        num_desired_instances = self._asg.desired_capacity
        num_desired_cores = num_desired_instances * self.config.instance_cores
        if num_desired_cores != total_num_valid_cores:
            LOG.debug("\tmismatching core counts")
            LOG.debug("\tnum_desired_cores: %d" % (num_desired_cores))
            LOG.debug("\ttotal_valid_cores: %d" % (total_valid_cores))
        return total_valid_cores

    def get_instance_by_id(self, id):
        LOG.debug("Searching for instance %s" % id)
        for instances in self.all_instances:
            if instance.id == id:
                LOG.debug("Found instance %s" % id)
                return instance
        return None

    def get_instance_ids_for_public_dns_names(self, public_dns_names):
        instance_ids = []
        for instance in self.all_instances:
            if instance.public_dns_name in public_dns_names:
                instance_ids.append(instance.id)
        return instance_ids

    def get_public_dns_names_close_to_charge(self):
        instances_close_to_charge = []
        sleep_secs = self.config.get_loop_sleep_secs()
        cur_utc_time = datetime.datetime.utcnow()
        valid_instances = self.get_valid_instances()
        time_fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
        for instance in valid_instances:
            launch_time = datetime.datetime.strptime(instance.launch_time,
                                                     time_fmt)
            time_diff_secs = (cur_utc_time - launch_time).total_seconds()
            cur_charge_secs = time_diff_secs % self.config.charge_time_secs
            secs_to_charge = self.config.charge_time_secs - cur_charge_secs
            LOG.debug("%s:%s: charge: %d; current: %d; to charge: %d" % (
                instance.id, instance.public_dns_name,
                self.config.charge_time_secs,
                cur_charge_secs, secs_to_charge))
            if secs_to_charge < (3 * sleep_secs):
                instances_close_to_charge.append(instance.public_dns_name)
        return instances_close_to_charge

    def delete_instances(self, instance_ids=[]):
        if not instance_ids:
            return
        LOG.debug("Deleting instances: %s" % instance_ids)
        # TODO(pdmars): this has the potential to kill instances running jobs
        # maybe I should err on the side of having extra instances if the
        # capacity is higher than the cloud can currently support
        num_instances = len(self.all_instances)
        if ((self._asg.desired_capacity > num_instances) and
                (num_instances > 0)):
            LOG.warn("Desired capacity is greater than num_instances running")
            LOG.warn("Adjusting desired capacity to match")
            self.set_capacity(num_instances)
        for instance_id in instance_ids:
            self._as_conn.terminate_instance(instance_id)
            # TODO(pdmars): due to a bug in phantom, maybe this will help
            # 2013/04/05: this might not be relevant anymore
            time.sleep(.1)

    def launch_autoscale_instances(self, num_instances=1):
        new_capacity = self._asg.desired_capacity + int(num_instances)
        if new_capacity > self.config.max_instances:
            new_capacity = self.config.max_instances
            LOG.warn("%s can launch %s total instances" % (self.config.name,
                                                           new_capacity))
        self._last_launch_attempt = datetime.datetime.utcnow()
        LOG.debug("Setting cloud capacity for %s to %s" % (self.config.name,
                                                           new_capacity))
        self.set_capacity(new_capacity)

    def set_capacity(self, new_capacity):
        self._asg.set_capacity(new_capacity)


class Clouds(object):
    def __init__(self, cloud_names, global_config):
        self.cloud_names = cloud_names
        self._global_config = global_config
        self.clouds = {}
        self._clouds_low_to_high = []
        self._instances_out_of_date = []
        self._initialize()

    def _create_cloud_from_config(self, name):
        return Cloud(CloudConfig(name, self._global_config))

    def _get_clouds_ordered_by_price(self, descending=False):
        clouds = self.clouds.values()
        sorted_clouds = sorted(clouds, key=lambda x: x.config.price,
                               reverse=descending)
        return sorted_clouds

    def _initialize(self):
        LOG.debug("Initializing all clouds")
        for name in self.cloud_names:
            c = self._create_cloud_from_config(name)
            self.clouds[name] = c
        LOG.debug("Sorting clouds by price (low to high)")
        self._clouds_low_to_high = self._get_clouds_ordered_by_price()

    def get_cheapest_valid_cloud(self):
        clouds = self._clouds_low_to_high
        for cloud in clouds:
            if (not cloud.failed_launch) and (not cloud.maxed):
                return cloud

    def get_clouds_low_to_high(self):
        return self._clouds_low_to_high

    def get_total_num_valid_cores(self):
        total_num_valid_cores = 0
        for cloud in self.get_clouds_low_to_high():
            total_num_valid_cores += cloud.get_total_num_valid_cores()
        return total_num_valid_cores

    def _update_cluster_instances(self, cluster):
        out_of_date = []
        cloud_dns_names = []
        clouds = self.get_clouds_low_to_high()
        for cloud in clouds:
            for instance in cloud.all_instances:
                cloud_dns_names.append(instance.public_dns_name)
        for node in cluster.nodes:
            if node.public_dns_name not in cloud_dns_names:
                LOG.debug("%s appears out of date" % node.public_dns_name)
                if node.public_dns_name in self._instances_out_of_date:
                    out_of_date.append(node.public_dns_name)
                else:
                    self._instances_out_of_date.append(node.public_dns_name)
            elif node.public_dns_name in cloud_dns_names:
                if node.public_dns_name in self._instances_out_of_date:
                    self._instances_out_of_date.remove(node.public_dns_name)
        LOG.debug("Instances no longer exist, removing: %s" % out_of_date)
        for public_dns_name in out_of_date:
            cluster.remove_node(public_dns_name)
            if public_dns_name in self._instances_out_of_date:
                self._instances_out_of_date.remove(public_dns_name)
        LOG.debug("Attempting to add new nodes")
        for cloud in clouds:
            for instance in cloud.all_instances:
                if instance.public_dns_name:
                    cluster.add_node(instance.public_dns_name,
                                     cloud.config.instance_cores)

    def refresh_all(self, cluster):
        for cloud_name in self.clouds.keys():
            self.clouds[cloud_name].refresh(cluster)
        self._update_cluster_instances(cluster)
