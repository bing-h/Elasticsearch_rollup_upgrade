"""
@version: python3.7.3
@author: bing.he
@contact: bing.he@ihandysoft.com
@file: rolling_upgrades_es.py
@time: 7/14/21 11:56 AM
"""
import json
import logging
import sys
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass
from logging import handlers

import paramiko
import yaml

logger = None


class Logger:
    def __init__(self):
        self._logger = logging.getLogger('rollup elasticsearch cluster')
        self._logger.setLevel('INFO')
        self._add_file_handler(self._logger, 'info.log')
        self._add_file_handler(self._logger, 'error.log', level=logging.ERROR)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self._logger.addHandler(ch)

    def __getattr__(self, item):
        return getattr(self._logger, item)

    def _add_file_handler(self, logger, filename, level=logging.INFO, when='D', backCount=3,
                          fmt='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'):
        def _handler(record):
            return record.levelno == level

        format_str = logging.Formatter(fmt)
        th = handlers.TimedRotatingFileHandler(filename=filename, when=when, backupCount=backCount,
                                               encoding='utf-8')
        th.setFormatter(format_str)
        th.setLevel(level)
        th.addFilter(_handler)
        logger.addHandler(th)


@dataclass
class ClusterInfo:
    ssh_private_key: str
    data_nodes: list
    master_nodes: list
    coordinate_nodes: list
    kibana_nodes: list
    es_host: str
    es_version: str
    kibana_version: str
    node_total: str

    def __init__(self):
        pass


def exec_command(ssh, command: str, stdin_list=None, ignore_err=False):
    """

    Parameters
    ----------
    ssh :ssh client.
    command :submit to remote shell to exec.
    stdin_list :if exec blocked and wait to input,use stdin_list to pass confirm info.
    ignore_err : sometimes warning info write into stderr and return,so we just ignore it.

    Returns out info
    -------

    """
    logger.info('begin exec command=,\n%s' % command)
    stdin, stdout, stderr = ssh.exec_command(
        command)
    if stdin_list:
        for std_str in stdin_list:
            stdin.write(std_str)
        stdin.flush()
    err = stderr.read().decode('utf-8')
    if not ignore_err and err and 'WARNING' not in err:
        raise Exception(err)
    out = stdout.read().decode('utf-8')
    logger.info('stdout=%s' % out)
    return out


@contextmanager
def connect_server(key_path, host_ip):
    ssh = None
    try:
        key = paramiko.RSAKey.from_private_key_file(key_path)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname=host_ip, username="ubuntu", pkey=key, timeout=10000)
        yield ssh
    except Exception as e:
        logger.error(repr(e))
    finally:
        if ssh:
            ssh.close()


def disable_shard_allocation(ssh, es_host):
    while True:
        out = exec_command(ssh, """
            curl -XPUT -u elastic:bytepower -H "Content-type: application/json"  'http://%s:9200/_cluster/settings?format=json' -d '{
              "persistent": {
                "cluster.routing.allocation.enable": "primaries"
              }
            }'
        """ % es_host, ignore_err=True)
        out_json = json.loads(out)
        if out_json.get('acknowledged'):
            return
        else:
            logger.info('wait disable_shard_allocation')
            time.sleep(20)


def reenable_shard_allocation(ssh, es_host):
    while True:
        out = exec_command(ssh, """
            curl -XPUT -u elastic:bytepower -H "Content-type: application/json"  'http://%s:9200/_cluster/settings' -d '{
              "persistent": {
                "cluster.routing.allocation.enable": null
              }
            }'
        """ % es_host, ignore_err=True)
        out_json = json.loads(out)
        if out_json.get('acknowledged'):
            return
        else:
            logger.info('wait reenable_shard_allocation')
            time.sleep(20)


def shutdown_node(ssh):
    exec_command(ssh, 'sudo systemctl stop elasticsearch.service')


def start_node(ssh):
    exec_command(ssh, 'sudo systemctl start elasticsearch.service')


def upgrade_plugins(ssh):
    exec_command(ssh, 'sudo /usr/share/elasticsearch/bin/elasticsearch-plugin remove repository-s3')
    exec_command(ssh, 'sudo /usr/share/elasticsearch/bin/elasticsearch-plugin remove opendistro_sql', ignore_err=True)
    exec_command(ssh, 'echo "y" | sudo /usr/share/elasticsearch/bin/elasticsearch-plugin install repository-s3')


def install_es(ssh, es_version):
    exec_command(ssh, """
    echo 'debconf debconf/frontend select Noninteractive' | sudo debconf-set-selections
    sudo apt-get -y install dialog""")
    exec_command(ssh, 'wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo apt-key  add -',
                 ignore_err=True)
    exec_command(ssh, 'sudo apt-get -y install apt-transport-https')
    exec_command(ssh,
                 'echo "deb https://artifacts.elastic.co/packages/7.x/apt stable main" | sudo tee /etc/apt/sources.list.d/elastic-7.x.list')
    exec_command(ssh, 'sudo apt-get update && sudo apt-get -y install elasticsearch={es_version}'.format(
        es_version=es_version), ['N\n', 'N\n'])
    exec_command(ssh, 'sudo chown -R elasticsearch:elasticsearch /esdata', ignore_err=True)
    exec_command(ssh, 'sudo chown -R elasticsearch:elasticsearch /mnt/log/es', ignore_err=True)


def wait_node_recover(ssh, node_total, es_host):
    while True:
        time.sleep(30)
        out = exec_command(ssh, """curl -XGET -u elastic:bytepower -H "Content-type: application/json"  'http://%s:9200/_cat/health?format=json'
    """ % es_host, ignore_err=True)
        out_json = json.loads(out)[0]
        if out_json.get('status') == 'green' and out_json.get('node.total') == node_total:
            break
        else:
            logger.info('wait cluster green status')


def install_kibana(ssh, kibana_version):
    exec_command(ssh, """ 
    echo 'debconf debconf/frontend select Noninteractive' | sudo debconf-set-selections
    sudo apt-get -y install dialog""")
    exec_command(ssh, 'sudo systemctl stop kibana')
    exec_command(ssh, 'sudo apt-get update && sudo apt-get -y install kibana={kibana_version}'.format(
        kibana_version=kibana_version), ['N\n'])
    exec_command(ssh, 'sudo systemctl start kibana')


def roll_up_node(ssh, es_version, node_total, es_host):
    disable_shard_allocation(ssh, es_host)
    shutdown_node(ssh)
    install_es(ssh, es_version)
    upgrade_plugins(ssh)
    start_node(ssh)
    reenable_shard_allocation(ssh, es_host)
    wait_node_recover(ssh, node_total, es_host)


def roll_up_cluster(cluster_info: ClusterInfo):
    # Warning: roll up  must start with data node ，end with master node
    private_key_path = cluster_info.ssh_private_key
    for data_node_ip in cluster_info.data_nodes:
        with connect_server(private_key_path, data_node_ip) as ssh:
            logger.info('begin roll up data node,ip=%s' % data_node_ip)
            roll_up_node(ssh, cluster_info.es_version, cluster_info.node_total, cluster_info.es_host)
            logger.info('roll up data node success,ip=%s' % data_node_ip)
    for coordinate_node_ip in cluster_info.coordinate_nodes:
        with connect_server(private_key_path, coordinate_node_ip) as ssh:
            logger.info('begin roll up coordinate node,ip=%s' % coordinate_node_ip)
            roll_up_node(ssh, cluster_info.es_version, cluster_info.node_total, cluster_info.es_host)
            logger.info('roll up coordinate node success,ip=%s' % coordinate_node_ip)
    for master_node_ip in cluster_info.master_nodes:
        with connect_server(private_key_path, master_node_ip) as ssh:
            logger.info('begin roll up master node,ip=%s' % master_node_ip)
            roll_up_node(ssh, cluster_info.es_version, cluster_info.node_total, cluster_info.es_host)
            logger.info('roll up master node success,ip=%s' % master_node_ip)
    for kibana_node_ip in cluster_info.kibana_nodes:
        with connect_server(private_key_path, kibana_node_ip) as ssh:
            logger.info('begin roll up kibana node,ip=%s' % kibana_node_ip)
            install_kibana(ssh, cluster_info.kibana_version)
            logger.info('roll up kibana node success,ip=%s' % kibana_node_ip)


def main():
    try:
        global logger
        logger = Logger()
    except Exception as e:
        logging.error('init error %s \ntraceback%s' % (repr(e), traceback.format_exc()))
        sys.exit(-1)
    try:
        config_path = sys.argv[1]
        cluster_info = ClusterInfo()
        with open(config_path, 'r', encoding='utf-8') as f:
            result = yaml.load(f)
            cluster_info.ssh_private_key = result.get('ssh_private_key')
            cluster_info.data_nodes = result.get('data_nodes') if result.get('data_nodes') else []
            cluster_info.master_nodes = result.get('master_nodes') if result.get('master_nodes') else []
            cluster_info.coordinate_nodes = result.get('coordinate_nodes') if result.get('coordinate_nodes') else []
            cluster_info.kibana_nodes = result.get('kibana_nodes') if result.get('kibana_nodes') else []
            cluster_info.es_host = result.get('es_host')
            cluster_info.es_version = result.get('es_version')
            cluster_info.kibana_version = result.get('kibana_version')
            cluster_info.node_total = result.get('node_total')
            logger.info(cluster_info)
        roll_up_cluster(cluster_info)
    except Exception as e:
        logger.error(repr(e), traceback.format_exc())


if __name__ == '__main__':
    main()
