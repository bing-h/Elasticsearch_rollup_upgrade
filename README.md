# elasticsearch_rollup_upgrade

You should read the upgrade tutorial carefully [elasticsearch doc](https://www.elastic.co/guide/en/elasticsearch/reference/current/setup-upgrade.html).

ES plugins should match ES version,list what plugins you installed in old version,and modify `upgrade_plugins` functions.

The script run successful based on below environment:

* Elasticsearch Version : elasticsearch 7.0+ upgrade to 7.13.3
* System: Ubuntu 18.04.1

