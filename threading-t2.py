#!/usr/bin/python
# -*- coding: UTF-8 -*-

import threading
import Queue
import time
import uuid
import redis
import time
import random
import simplejson as json

redis_option = {
    'host'    : '127.0.0.1',
    'port'    : 6379,
    'db'      : 6,
    'password': None,
}

redis_client = redis.Redis(**redis_option)

delay_queue_test = 'delay_queue:test:'
timely_queue_test = 'timely_queue:test'


def add_delay_task(task, delay_time):
    """添加延时任务"""
    redis_client.zadd(delay_queue_test, task, delay_time)

def add_timely_task(task):
    """添加及时任务"""
    redis_client.lpush(timely_queue_test, [task])


def get_delay_task():
    """获取延时任务"""
    redis_result = redis_client.zrange(delay_queue_test, 0, 0, withscores=True)
    if redis_result:
        cc = redis_result[0][1]
        item_str = redis_result[0][0]

        # 没有符合时间点的任务直接返回None
        if time.time() < cc:
            return None

        # 提取这个符合时间条件的任务
        task = redis_client.zrem(delay_queue_test, item_str)
        return task
    else:
        return None

def get_timely_task():
    """获取及时任务"""
    return redis_client.rpop(timely_queue_test)



def worker_analysis_resource_create_task(task):
    """分析资源创建任务的线程"""
    print "A---start--->{}".format(time.ctime(time.time()))
    time.sleep(1)
    print "A---end----->{}".format(time.ctime(time.time()))
    # semaphore.release()

def worker_resource_polling_task(task):
    """资源状态轮训任务的线程"""
    print "B---start--->{}".format(time.ctime(time.time()))
    time.sleep(5)
    print "B---end----->{}".format(time.ctime(time.time()))
    # semaphore.release()


def worker_resource_releasing_task():
    """资源释放任务的线程"""
    pass


def check_semaphore(thread_cfg):
    result = {}
    keys = thread_cfg.keys()

    thread_names = [x.getName().split('-')[0] for x in threading.enumerate()]
    for k in keys:
        result[k] =   True  if thread_names.count(k) < thread_cfg[k] else False

    return result


if __name__ == '__main__':

    # 模拟添加延迟任务
    for t in ['task-delay-{}'.format(i) for i in range(20)]:
        # cc = time.time() + random.randrange(1,20)
        cc = time.time() + 0
        add_delay_task(t, cc)


    # 模拟添加即时任务
    for t in ['task-{}'.format(i) for i in range(20)]:
        add_timely_task(t)


    semaphore_cfg = {
        'tt1' : 2,
        'tt2' : 3
    }

    # 模拟单元任务执行
    run_tag = True
    n=0
    while run_tag:
        thread_list = []
        semaphore_info = check_semaphore(semaphore_cfg)
        print "T--------------------------------------1---->{}---{}-------{}".format(n, time.ctime(time.time()), threading.activeCount())
        # 即时任务
        if semaphore_info['tt1']:
            task = get_timely_task()
            t = threading.Thread(name='tt1-{}'.format(uuid.uuid4().hex), target=worker_analysis_resource_create_task, args=(task,))
            thread_list.append(t)
        print "T--------------------------------------2---->{}---{}-------{}".format(n, time.ctime(time.time()), threading.activeCount())

        # 延时任务
        print "f----0---->{}".format(time.time())
        if semaphore_info['tt2']:
            print "f----1---->{}".format(time.time())
            task = get_delay_task()
            t = threading.Thread(name='tt2-{}'.format(uuid.uuid4().hex), target=worker_resource_polling_task, args=(task,))
            thread_list.append(t)
        print "T--------------------------------------3---->{}---{}-------{}".format(n, time.ctime(time.time()), threading.activeCount())

        for t in thread_list:
            t.start()
        print "T--------------------------------------4---->{}---{}-------{}".format(n, time.ctime(time.time()), threading.activeCount())
        time.sleep(1)
        n=n+1
        if n>100:
            break


"""
后期外部可以通过动态的设置  semaphore_cfg 值即可达到目的
"""








