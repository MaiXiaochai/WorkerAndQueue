# -*- encoding: utf-8 -*-

"""
------------------------------------------
@File       : worker_queue_utils.py
@Author     : maixiaochai
@Email      : maixiaochai@outlook.com
@CreatedOn  : 2021/6/17 10:32
------------------------------------------
原作者代码：https://github.com/bslatkin/effectivepython/blob/master/example_code/item_55.py
"""
from queue import Queue
from threading import Thread, Lock


class CloseableQueue(Queue):
    """
        比较高级的队列用法
        Tips:
            Queue.get()会持续阻塞，直到队列中 put数据才返回
    """
    SENTINEL = object()

    def close(self):
        self.put(self.SENTINEL)

    def __iter__(self):
        while True:
            # 因为queue.get会持续阻塞，所以，在流程中并不会造成CPU时间的浪费
            item = self.get()

            # 这里用 try... finally...的写法是利用了即使 try中 return执行，finally中的语句也会执行、
            # 并且是在return返回结果之前执行的特性。
            # 这样，每次从queue中取出任务后，都会给queue发一个task_done的提示
            try:
                if item is self.SENTINEL:
                    return  # 让线程退出

                yield item

            finally:
                self.task_done()  # 给queue发送信号，停止阻塞


class StoppableWorker(Thread):
    """
        比较高级的 Worker
    """

    def __init__(self, func, in_queue, out_queue):
        super().__init__()
        self.func = func
        self.in_queue = in_queue
        self.out_queue = out_queue

    def run(self):
        for item in self.in_queue:
            result = self.func(item)
            self.out_queue.put(result)


def start_threads(count, *args):
    threads = [StoppableWorker(*args) for _ in range(count)]
    for thread in threads:
        thread.start()

    return threads


def stop_thread(closable_queue, threads):
    for _ in threads:
        closable_queue.close()

    # 实际上意味着等到队列为空，再执行别的操作
    closable_queue.join()

    # 等待所有线程结束再推出
    for thread in threads:
        thread.join()
