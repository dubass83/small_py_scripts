#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# bug-report: makssych@gmail.com
'''
Send Redis usage metrics to Amazon CloudWatch

'''
import requests
import redis
import boto3
_METADATA_URL = "http://169.254.169.254/latest/meta-data"


# Create CloudWatch client
cloudwatch = boto3.client('cloudwatch', region_name='eu-central-1')
curr_metrics = []

command_groups = {
    'GetTypeCmds': ['get','getbit','getrange','getset','mget','hget','hgetall','hmget'],
    'SetTypeCmds': ['set','setbit','setex','setnx','setrange','mset','msetnx','psetnx',
                    'hmset','hset','hsetnx','lset'],
    'KeyBasedCmds': ['zdel','dump','exists','expire','expireat','keys','move','persist',
                     'pexpire','pexpireat','pttl','rename','renamenx','restore','ttl',
                     'type','append','bitcount','bitop','bitpos','decr','decrby','get',
                     'getbit','getrange','getset','incr','incrby','incrbyfloat','mget',
                     'mset','msetnx','psetnx','set','setbit','setex','setnx','setrange',
                     'strlen','hdel','hexists','hget','hgetall','hincrby','hincrbyfloat',
                     'hkeys','hlen','hmget','hmset','hset','hsetnx','hvals','blpop',
                     'brpop','lindex','linsert','llen','lpop','lpush','lpushx','lrange',
                     'lrem','lset','ltrim','rpop','rpush','rpushx','sadd','scard','sdiff',
                     'sdiffstore','sinter','sinterstore','sismember','smembers','spop',
                     'srandmember','srem','sunion','sunionstore', 'sscan','zadd','zcard',
                     'zcount','zincrby','zinterstore','zlexcount','zrange','zrangebylex',
                     'zrangebyscore','zrank','zrem','zremrangebylex','zremrangebyrank',
                     'zremrangebyscore','zrevrange','zrevrangebyscore','zrevrank','zscore',
                     'zunionstore','zscan','pfadd','pfcount','pfmerge','watch','eval',
                     'evalsha'],
    'StringBasedCmds': ['append','bitcount','bitop','bitpos','decr','decrby','get','getbit',
                        'getrange','getset','incr','incrby','incrbyfloat','mget','mset',
                        'msetnx','psetnx','set','setbit','setex','setnx','setrange','strlen'],
    'HashBasedCmds': ['hdel','hexists','hget','hgetall','hincrby','hincrbyfloat','hkeys',
                      'hlen','hmget','hmset','hset','hsetnx','hvals','hscan'],
    'ListBasedCmds': ['blpop','brpop','brpoplpush','lindex','linsert','llen','lpop','lpush',
                      'lpushx','lrange','lrem','lset','ltrim','rpop','rpoplpush','rpush',
                      'rpushx'],
    'SetBasedCmds': ['sadd','scard','sdiff','sdiffstore','sinter','sinterstore','sismember',
                     'smembers','smove','spop','srandmember','srem','sunion','sunionstore',
                     'sscan'],
    'SortedSetBasedCmds': ['zadd','zcard','zcount','zincrby','zinterstore','zlexcount',
                           'zrange','zrangebylex','zrangebyscore','zrank','zrem',
                           'zremrangebylex','zremrangebyrank','zremrangebyscore','zrevrange',
                           'zrevrangebyscore','zrevrank','zscore','zunionstore','zscan'],
    'HyperLogLogBasedCmds': ['pfadd','pfcount','pfmerge'],
    'ScriptBasedCmds': ['eval','evalsha']
}

def collect_redis_info():
    r = redis.StrictRedis('localhost', port=6379, db=0)
    res_dict = {}
    info = r.info()
    cmd_info = r.info('commandstats')
    res_dict = info.copy()
    res_dict.update(cmd_info)
    return res_dict


def append_metric(metricName, dimensions, val, unit='Count'):
    m_data = {
        'MetricName': metricName,
        'Dimensions': dimensions,
        'Unit': unit,
        'Value': val
        # 'Timestamp': datetime.datetime.now(),
        # 'StatisticValues': {
        #     'SampleCount': val,
        #     'Sum': val,
        #     'Minimum': val,
        #     'Maximum': val
        # }
    }
    print(m_data)
    curr_metrics.append(m_data)

if __name__ == '__main__':
    instance_id = requests.get( _METADATA_URL + '/instance-id').text
    redis_data = collect_redis_info()
    dimensions = [{'Name' : 'Redis', 'Value': instance_id}]

    count_metrics = {
        # 'CurrConnections': redis_data['connected_clients'],
        # 'Evictions': redis_data['evicted_keys'],
        # 'Reclaimed': redis_data['expired_keys'],
        # 'CacheHits': redis_data['keyspace_hits'],
        # 'CacheMisses': redis_data['keyspace_misses'],
        #'UsedMemory': redis_data['used_memory'],
        'IOPS': redis_data['instantaneous_ops_per_sec'],
        # 'InputKbps': redis_data['instantaneous_input_kbps'],
        # 'OutputKbps': redis_data['instantaneous_output_kbps'],
    }

    # count_metrics['CurrItems'] = sum([value['keys'] for key, value in redis_data.items() if key.startswith('db')])

    # for command_group, commands in command_groups.items():
    #     count_metrics[command_group] = 0
    #     for command in commands:
    #         key = 'cmdstat_' + command
    #         if key in redis_data:
    #             count_metrics[command_group] += redis_data[key]['calls']

    byte_metrics = {
        'BytesUsedForCache': redis_data['used_memory'],
    }

    # append_metric('UsedMemory', dimensions, val=count_metrics['UsedMemory'], unit='Count')
    append_metric('IOPS', dimensions, val=count_metrics['IOPS'], unit='Count')
    append_metric('BytesUsedForCache', dimensions, val=byte_metrics['BytesUsedForCache'], unit='Bytes')

    if len(curr_metrics) != 0:
        response = cloudwatch.put_metric_data(MetricData = curr_metrics, Namespace='Services')
        print(response)
