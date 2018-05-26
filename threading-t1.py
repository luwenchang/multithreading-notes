#!/usr/bin/python
# -*- coding: UTF-8 -*-

import threading
import Queue
import time
import datetime
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




def worker_analysis_resource_create_task(task, semaphore):
    """分析资源创建任务的线程"""
    print "A---start--->{}".format(time.ctime(time.time()))
    time.sleep(1)
    print "A---end----->{}".format(time.ctime(time.time()))
    semaphore.release()

def worker_resource_polling_task(task, semaphore):
    """资源状态轮训任务的线程"""
    print "B---start--->{}".format(time.ctime(time.time()))
    time.sleep(5)
    print "B---end----->{}".format(time.ctime(time.time()))
    semaphore.release()


if __name__ == '__main__':

    # 模拟添加延迟任务
    for t in ['task-delay-{}'.format(i) for i in range(20)]:
        # cc = time.time() + random.randrange(1,20)
        cc = time.time() + 0
        add_delay_task(t, cc)

    # 模拟添加即时任务
    for t in ['task-{}'.format(i) for i in range(20)]:
        add_timely_task(t)

    # 通过 Semaphore 限制各个工作单元的 线程数量限制
    a_semaphore = threading.Semaphore(4)
    b_semaphore = threading.Semaphore(2)

    # 模拟任务分发中心运行
    run_tag = True
    n=0
    while run_tag:
        thread_list = []
        print "T--------------------------------------1---->{}---{}-------{}".format(n, time.ctime(time.time()), threading.activeCount())
        # 及时任务处理
        if a_semaphore.acquire(blocking=False):
            task = get_timely_task()
            t = threading.Thread(name='tt1-{}'.format(uuid.uuid4().hex), target=worker_analysis_resource_create_task, args=(task, a_semaphore,))
            thread_list.append(t)
        print "T--------------------------------------2---->{}---{}-------{}".format(n, time.ctime(time.time()), threading.activeCount())

        # 延时任务处理
        print "f----0---->{}".format(time.time())
        if b_semaphore.acquire(blocking=False):
            print "f----1---->{}".format(time.time())
            task = get_delay_task()
            t = threading.Thread(name='tt2-{}'.format(uuid.uuid4().hex), target=worker_resource_polling_task, args=(task, b_semaphore,))
            thread_list.append(t)
        print "T--------------------------------------3---->{}---{}-------{}".format(n, time.ctime(time.time()), threading.activeCount())

        for t in thread_list:
            t.start()
        print "T--------------------------------------4---->{}---{}-------{}".format(n, time.ctime(time.time()), threading.activeCount())

        n=n+1
        if n>100:
            break


'''
经过测试发现：
    1.当 b_semaphore 的线程额度用满之后，b_semaphore.acquire() 会阻塞并等待有有空余资源之后再返回结果。可通过参数 blocking=False 取消阻塞，让其直接返回是否可新增线程
    2.追加 blocking=False 之后不会阻塞； 这种情况下应该可以做一个总的任务分发中心，然后可以以插件的形式添加任务
    3.此处有一个缺陷，这种方式无法动态的控制各个任务单元的线程数量。通过下一个脚本实现该问题
'''









