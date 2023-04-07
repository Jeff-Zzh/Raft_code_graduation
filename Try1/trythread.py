import threading
import time

exitFlag = 0

# class myThread (threading.Thread):
#     def __init__(self, threadID, name, delay):
#         threading.Thread.__init__(self)
#         self.threadID = threadID
#         self.name = name
#         self.delay = delay
#     def run(self):
#         print ("开始线程：" + self.name)
#         print_time(self.name, self.delay, 5)
#         print ("退出线程：" + self.name)
#
# def print_time(threadName, delay, counter):
#     while counter:
#         if exitFlag:
#             threadName.exit()
#         time.sleep(delay)
#         print ("%s: %s" % (threadName, time.ctime(time.time())))
#         counter -= 1
#
# # 创建新线程
# thread1 = myThread(1, "Thread-1", 1)
# thread2 = myThread(2, "Thread-2", 2)
#
# # 开启新线程
# thread1.start()
# thread2.start()
# thread1.join()  # 调用该方法将会使主调线程堵塞，直到被调用线程运行结束或超时
# thread2.join()
# print ("退出主线程")

def show(msg):
    time.sleep(1)
    for i in range(5):
        print('thread ' + str(msg) + ' running....' + str(i) + '\n')

# for i in range(10):
#     t1 = threading.Thread(target=show, args=(i,))
#     t2 = threading.Thread(target=show, args=(i + 10,))
#     t1.start()
#     t2.start()
#     t1.join()
#     t2.join()

t1 = threading.Thread(target=show, args=(1,))
t2 = threading.Thread(target=show, args=(2,))
t1.start()
t2.start()