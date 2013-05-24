Phorque
=======

Phorque monitors a T<b>orque</b> cluster, executes a policy to determine how many instances to launch or terminate, and then provisions instances on infrastructure clouds via __Ph__antom, an open source auto-scaling service that uses Amazon's auto-scaling API. You can find out more about Phantom here:

http://www.nimbusproject.org/phantom

And, please, fork Phorque.

Running Phorque
---------------

Install it:

    python setup.py build
    python setup.py install

For a list of options:

    phorque.py -h
    
Start it up:

    phorque.py

However, usually I run it like so (-d is for debug mode):

    phorque.py -d 2>&1 | tee phorque.log


Configuring Phorque
-------------------

Phorque's configuration file is divided into three sections: [Phorque], [Policy], and [Cloud-Name].

[Phorque] has the following options:

    loop_sleep_secs = 120
    cluster_directory = /opt/torque-3.0.6/
    queue_name = default

>_loop\_sleep\_secs_ is the number of seconds to sleep between each iteration when it queries the cluster queue and the cloud for updates.

>_cluster\_directory_ is the directory for the cluster software.

>_queue\_name_ is the name of the queue to query.

[Policy] has the following options:

    name = OnDemandPlusPlus
    price_per_hour = 5
    multiplier = 1

>_name_ is the name of the policy to use. It must map to a class name in policy/policies.py.

>_price\_per\_hour_ is the maximum amount of money the policy is allowed to spend per hour (if applicable).

>_multiplier_ is a value that's multiplied by the number of instances the policy attempts to launch. So if, for example, the policy determines it should launch 2 instance but multiplier is set to be 8 then 16 instances are launched.

[Cloud-Name] can be specified any number of times (make sure to change Name) and has the following options:

    cloud_uri = svc.uc.futuregrid.org
    cloud_port = 8444
    autoscale_uri = svc.uc.futuregrid.org
    autoscale_port = 8445
    image_id = debian-6.0.5.gz
    price = 0
    access_id = $ACCESS_ID
    secret_key = $SECRET_KEY
    launch_config_name = hotellc@hotel
    autoscale_group_name = hotelasg
    cloud_type = nimbus
    availability_zone = us-east-1
    instance_type = m1.large
    instance_cores = 2
    max_instances = 1024
    charge_time_secs = 3600
    user_data_file = /etc/phorque/user-data

>_cloud\_uri_ is the URI for the cloud.

>_cloud\_port_ is the port for the cloud.

>_autoscale\_uri_ is the uri for the auto-scale service.

>_autoscale\_port_ is the port for the auto-scale service.

>_image\_id_ is the name of the image to launch.

>_price_ is the price of the image that will be launched.

>_access\_id_ is the access ID key for the cloud.

>_secret\_key_ is the secret key for the cloud.

>_launch\_config\_name_ is the name of the launch configuration to create.

>_autoscale\_group\_name_ is the name of the auto-scale group to create.

>_cloud\_type_ is the type of cloud (e.g., nimbus).

>_availability\_zone_ is the cloud availability zone to use.

>_instance\_type_ is the size of the instance to launch.

>_instance\_cores_ is the number of cores the instance\_type will launch.

>_max\_instances_ is the maximum number of instances Phorque can launch.

>_charge\_time\_secs_ is the time (in seconds) that instances are charged by the cloud provider (if applicable).


Assumptions
-----------

Obviously, because Phorque dynamically launches and terminates instances on infrastructure clouds, you need a mechanism to ensure all nodes in the Torque cluster know about each other and trust each other. Typically this is done via exchanging IP addresses, hostnames, and SSH public keys. Unfortunately, Phorque does not currently provide this capability and therefore you must use your own solution (e.g., burn keys into a image on a trusted cloud, develop a set of scripts to exchange this information at boot, etc.).
