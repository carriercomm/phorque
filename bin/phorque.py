import logging
import os
import signal
import sys
import time

from cloud.clouds import Clouds
from cluster.torque import TorqueCluster
from lib.logger import configure_logging
from lib.util import parse_options
from lib.util import read_config
from policy import policies
from threading import Thread


SIGEXIT = False
STATIC_CONFIG_SECTIONS = ["Phorque", "Policy"]
LOG = logging.getLogger(__name__)


class Phorque(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self.config = config
        self.loop_sleep_secs = config.getint("Phorque", "loop_sleep_secs")
        self.cluster_directory = config.get("Phorque", "cluster_directory")
        self.cloud_names = list(set(config.sections()) -
                                set(STATIC_CONFIG_SECTIONS))
        self.policy_name = config.get("Policy", "name")
        Policy = getattr(policies, self.policy_name)
        self.policy = Policy()

    def _loop(self, cluster, clouds):
        while not SIGEXIT:
            try:
                LOG.debug("Attempting to update cluster information")
                cluster.update()
                LOG.info("Successfully updated cluster information")
            except Exception as e:
                LOG.error("Error updating cluster information: %s" % str(e))
            try:
                LOG.debug("Refreshing all clouds")
                clouds.refresh_all(cluster)
                LOG.info("Successfully refreshed all clouds")
            except Exception as e:
                LOG.error("Error refreshing cloud information: %s" % str(e))
            try:
                LOG.debug("Executing the policy")
                self.policy.execute(cluster, clouds)
                LOG.info("Successfully executed the policy")
            except Exception as e:
                LOG.error("Error executing the policy: %s" % str(e))
            LOG.info("Sleeping for %s seconds" % self.loop_sleep_secs)
            time.sleep(self.loop_sleep_secs)

    def run(self):
        LOG.debug("Configuring cluster: %s" % self.cluster_directory)
        if os.path.exists(self.cluster_directory):
            cluster = TorqueCluster(self.cluster_directory)
        else:
            LOG.error("Directory not found: %s" % self.cluster_directory)
            cluster = None
        LOG.debug("Loading cloud information from the database")
        try:
            clouds = Clouds(self.cloud_names, self.config)
        except Exception as e:
            LOG.error("Problem setting up clouds defined in the config file.")
            LOG.error("Please verify that the config file is correct.")
            LOG.error("Output: %s" % str(e))
            clouds = None
        if cluster and clouds:
            self._loop(cluster, clouds)
        else:
            LOG.error("Unable to start. Please fix your setup.")


def clean_exit(signum, frame):
    global SIGEXIT
    SIGEXIT = True
    LOG.critical("Exiting at the next possible time. Please stand by.")


def main():
    (options, args) = parse_options()
    configure_logging(options.debug)
    config = read_config(options.config_file)
    signal.signal(signal.SIGINT, clean_exit)
    phorque = Phorque(config)
    LOG.info("Starting Phorque thread")
    phorque.daemon = True
    phorque.start()
    # wake every second to make sure signals can be handled by the main thread
    while phorque.isAlive():
        phorque.join(timeout=1.0)

if __name__ == "__main__":
    main()
