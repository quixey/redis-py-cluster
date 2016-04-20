#!/usr/bin/python

import logging

from rediscluster.exceptions import RedisClusterUninitialized
from rediscluster import StrictRedisCluster
from redis import StrictRedis
import redistrib.command

NUM_SLOTS = 16384


def make_parts(n):
    p = NUM_SLOTS/n
    remain = NUM_SLOTS-(p*n)

    partitions = []
    i = 0
    count = 0
    s = remain
    while count < n:
        q = i
        r = i + p - 1
        if s:
            r += 1
            s -= 1
        partitions.append((q, r))
        i = (r + 1)
        count += 1
    return partitions


def create(startup_nodes, replicas=0):
    """
    Function to create a new cluster ONLY from nodes not already initialized. NOTE: this function replicates
    redis-trib.rb 'create' command, EXCEPT that it can take less than 3 nodes for initialization.
    """

    nodeset = dict()
    new_nodes = []
    for node in startup_nodes:
        try:
            rc = StrictRedisCluster(startup_nodes=[node], decode_responses=True)
            cluster_nodes = rc.cluster_nodes()
            for n in cluster_nodes:
               nodeset.update({n['id']: n})
        except RedisClusterUninitialized:
            new_nodes.append(node)

    if nodeset:
        logging.debug('nodes already in a cluster:')
        for n in nodeset:
            logging.debug( nodeset[n] )

    if not new_nodes:
        logging.debug( "no nodes available to be in a cluster" )
        return

    logging.debug( 'nodes to make a new cluster' )
    node_list = []
    for n in new_nodes:
        logging.debug( n )
        node_list.append((n['host'], int(n['port'])))

    master_count = len(node_list)
    if replicas:
        master_count /= (replicas + 1)
        if master_count < 1:
            logging.debug( "ERROR: not enough fresh nodes to accomodate replication factor of {}".format(replicas) )
            return

    master_count = int(master_count)
    master_list = node_list[:master_count]
    slave_list = node_list[master_count:]

    if len(master_list) > 1:
        logging.debug( "INFO: creating cluster with the following nodes as masters: {}".format(master_list) )
        redistrib.command.start_cluster_on_multi(master_list)
    else:
        host = master_list[0][0]
        port = master_list[0][1]
        logging.debug( "INFO: creating single master: {} {}".format(host, port) )
        m = StrictRedis(host=host, port=port)
        s = ""
        for i in xrange(1, NUM_SLOTS):
            s += "{} ".format(i)
        cmd = 'CLUSTER ADDSLOTS {}'.format(s)
        logging.debug( "INFO: sending following command: {}".format(cmd) )
        m.execute_command(cmd)


    # add slaves
    if replicas:
        logging.debug( "INFO: adding following nodes as slaves evenly across masters: {}".format(slave_list) )
        for i, s in enumerate(slave_list):
            m = master_list[i % master_count]
            redistrib.command.replicate(m[0], m[1], s[0], s[1])
    return True


def _map_cluster(node_host, node_port):
    cluster = {}
    slaves = []
    node_port = int(node_port)
    nodes, master = redistrib.command.list_nodes(node_host, node_port)
    for node in nodes:
        if 'master' in node.role_in_cluster:
            if node.node_id not in cluster:
                cluster[node.node_id] = {'self': node, 'slaves': []}
        else:
            slaves.append(node)

    for slave in slaves:
        cluster[slave.master_id]['slaves'].append(slave)

    if slaves:
        cluster_replication_factor = int(len(cluster) / len(slaves))
    else:
        cluster_replication_factor = 0
    return cluster, slaves, cluster_replication_factor


def expand_cluster(master_host, master_port, new_nodes, num_new_masters=None):
    """
    function to add a set of nodes to an existing cluster. NOTE: this function presumes that the list of new nodes are
     NOT present on existing instances. Future versions MAY try to re-balance slaves among IP Addresses.
    :param master_host: machine IP (string) of any healthy cluster node
    :param master_port: machine Port of the master_host
    :param new_nodes: list of {"host": "[ip_addr]", "port": "[port_num]"} objects
    :param num_new_masters: if you want a specific ammount to be new masters, set this parameter Default: function will
        attempt to auto-detect existing replication factor, and populate, accordingly.
    :return:
    """

    cluster, slaves, cluster_replication_factor = _map_cluster(master_host, master_port)
    logging.debug( "cluster: {}".format(cluster) )
    logging.debug( "slaves: {}".format(slaves) )

    if not cluster:
        logging.debug( "ERROR: Empty Cluster for Host {} {}".format(master_host, master_port) )
        return

    num_sets_to_add = int(len(new_nodes)/(cluster_replication_factor + 1))
    if not num_sets_to_add:
        logging.debug( "ERROR: Cluster has a replication factor of {}. Insufficient number of new nodes given." )
        return

    new_replication_factor = cluster_replication_factor
    if num_new_masters:
        if num_new_masters < len(new_nodes):
            logging.debug( "ERROR: Insufficient number of new nodes ({}) to accomodate number of masters requested ({})".format(
                len(new_nodes), num_new_masters
            ) )
            return
        num_sets_to_add = min(num_sets_to_add, num_new_masters)
        new_replication_factor = min(cluster_replication_factor, (len(new_nodes)/(num_new_masters + 1)))

    logging.debug( "crf: {}".format(new_replication_factor) )

    master_list = None

    while num_sets_to_add:
        if not master_list:
            master_list = cluster.values()
            logging.debug( "master list: {}".format(master_list ))
        num_sets_to_add -= 1
        new_master_node = new_nodes.pop()
        master = master_list.pop()
        existing_master_node = master['self']
        existing_slave_nodes = master['slaves']
        logging.debug( 'adding new node {} to cluster as master, with {}.'.format(new_master_node, existing_master_node.port) )

        redistrib.command.join_cluster(existing_master_node.host, existing_master_node.port, 
                                       new_master_node['host'], int(new_master_node['port']))
        logging.debug( 'master node joined to cluster!' )

        for j in range(new_replication_factor):
            new_slave_node = new_nodes.pop()
            existing_slave_node = existing_slave_nodes.pop()
            logging.debug( 'adding node {} as slave to master {}'.format(new_slave_node['port'], existing_master_node.port) )
            redistrib.command.replicate(existing_master_node.host, existing_master_node.port, new_slave_node['host'], int(new_slave_node['port']))
            if existing_slave_node:
                logging.debug( 'switching existing slave node {} to master {}'.format(existing_slave_node.port, new_master_node['port'] ) )

                redistrib.command.replicate(new_master_node['host'], int(new_master_node['port']), existing_slave_node.host, existing_slave_node.port)
            else:
                logging.debug( "WARN: slave node underrun for replication {} of factor {}".format(j+1, new_replication_factor) )
            logging.debug( "slave node {} added to the master".format(existing_slave_node) )

    while new_nodes:
        logging.debug( "there are new nodes left over, so adding them as slaves")
        cluster, slaves, new_replication_factor = _map_cluster(master_host, master_port)
        underweight = []
        for master in cluster.values():
            if len(master['slaves']) < new_replication_factor:
                underweight.append(master)

        if underweight:
            while underweight and new_nodes:
                master = underweight.pop()
                existing_master_node = master['self']
                new_master_node = new_nodes.pop()
                redistrib.command.replicate(existing_master_node.host, existing_master_node.port, new_master_node['host'], int(new_master_node['port']))
        else:
            master_nodes = cluster.values()
            while master_nodes and new_nodes:
                new_master_node = new_nodes.pop()
                existing_master_node = master_nodes.pop()
                redistrib.command.replicate(existing_master_node.host, existing_master_node.port, new_master_node['host'], int(new_master_node['port']))


def main():
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    # startup_nodes = [{"host": "10.64.1.102", "port": "7000"}]
    startup_nodes = [{"host": "192.168.99.100", "port": "6380"}, {"host": "192.168.99.100", "port": "6381"}]
    create(startup_nodes, replicas=1)
    expand_cluster(startup_nodes[0]['host'], startup_nodes[0]['port'], [{'host': "192.168.99.100", "port":"6382"},
                                                                        {'host': "192.168.99.100", "port":"6383"}])



if __name__ == "__main__":
    main()

