from rediscluster.cluster_mgt import RedisClusterMgt
from rediscluster import StrictRedisCluster


def main(startup_nodes):

    # Note: decode_responses must be set to True when used with python3
    rc = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    resp = rc.cluster_info()


def main1(startup_nodes):

    r = RedisClusterMgt(startup_nodes)
    resp = r.slots()
    print resp

if __name__ == "__main__":
    startup_nodes = [{"host": "10.64.1.102", "port": "7000"}]
    main(startup_nodes)
