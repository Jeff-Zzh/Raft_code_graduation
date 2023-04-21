import time
from pysyncobj import SyncObj, replicated
from pysyncobj.config import SyncObjConf

class Person:  # class Person(object):
    def __init__(self, name, age, sex):
        self.__name = name
        self.__age = age
        self.__sex = sex
    def get_info(self):
        print('name: ', self.__name)
        print('name: ', self.__age)
        print('sex: ', self.__sex)
        return [self.__name, self.__age, self.__sex]
    def __getattr__(self, item):
        print(f'{item} 不存在，或不是public权限，你访问不了！！！')
        return None  # 输出 print 返回 None

class Student(Person):
    def __init__(self, stu_ID, name, age, sex):
        super().__init__(name, age, sex)
        self.__stu_ID = stu_ID
        self.__name = name
        self.__age = age
        self.__sex = sex

    def get_info(self):  # 重写父类方法
        print('stu_ID: ', self.__stu_ID)
        print('name: ', self.__name)
        print('name: ', self.__age)
        print('sex: ', self.__sex)
        return [self.__stu_ID, self.__name, self.__age, self.__sex]

# p1 = Person('zzh', 22, '男')
# print(p1.__name)  # __name 不存在，或不是public权限，你访问不了！！！  None
# s1 = Student(19170100067, 'zzh', 22, '男')
# print(s1.__stu_ID)  # __stu_ID 不存在，或不是public权限，你访问不了！！！  None

s1 = Student(19170100067, 'zzh', 22, '男')
