import os


VALID_RUN_STATES = ["running", "pending"]


class CloudConfig(object):
    def __init__(self, name, config):
        self.name = name
        self._config = config
        self.image_id = self._config.get(name, "image_id")
        self.lc_name = self._config.get(name, "launch_config_name")
        self.asg_name = self._config.get(name, "autoscale_group_name")
        self.price = self._config.getfloat(name, "price")
        self.cloud_uri = self._config.get(name, "cloud_uri")
        self.cloud_port = self._config.getint(name, "cloud_port")
        self.as_uri = self._config.get(name, "autoscale_uri")
        self.as_port = self._config.getint(name, "autoscale_port")
        self.cloud_type = self._config.get(name, "cloud_type")
        self.az = self._config.get(name, "availability_zone")
        self.instance_type = self._config.get(name, "instance_type")
        self.instance_cores = self._config.getint(name, "instance_cores")
        self.max_instances = self._config.getint(name, "max_instances")
        self.charge_time_secs = self._config.getint(name, "charge_time_secs")
        access_id = self._config.get(name, "access_id")
        try:
            self.access_id = os.environ[access_id.lstrip('$')]
        except KeyError:
            self.access_id = access_id
        secret_key = self._config.get(name, "secret_key")
        try:
            self.secret_key = os.environ[secret_key.lstrip('$')]
        except KeyError:
            self.secret_key = secret_key

    def get_loop_sleep_secs(self):
        return self._config.getint("Phorque", "loop_sleep_secs")
