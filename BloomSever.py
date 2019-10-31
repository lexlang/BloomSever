from bloom_filter import BloomFilter
import pickle
import time
import threading
import os
from flask import Flask, request,jsonify

bloomStore={}
lock = threading.Lock() #锁
application = Flask(__name__)

@application.route('/check', methods=['GET'])
def check():
    store = request.values.get('store')
    value = request.values.get('value')
    return jsonify({"access":"success","check":checkExists(store,value)})

@application.route('/add', methods=['GET'])
def add():
    store = request.values.get('store')
    value = request.values.get('value')
    maxElement = request.values.get('maxElement')
    if maxElement!=None:
        addBloom(store,value,int(maxElement))
    else:
        addBloom(store, value)
    return jsonify({"access":"add_success"})

@application.route('/remove', methods=['GET'])
def remove():
    store = request.values.get('store')
    removeKeyBloom(store)
    return jsonify({"access":"remove_success"})

def createNewBloomFilter(maxElement):
    '''
    创建千万级布隆过滤
    :return:
    '''
    return BloomFilter(max_elements=maxElement, error_rate=0.001)

def checkExists(key,value):
    '''
    检查值是否采集
    :param key:
    :param value:
    :return:
    '''
    if key in bloomStore:
        bl=bloomStore[key]
        if value in bl:
            return True
        else:
            return False
    else:
        return False

def addBloom(key,value,maxElement=10000000):
    '''
    加入布隆过滤
    :param key:
    :param value:
    :return:
    '''
    with lock:
        if key in bloomStore:
            bl=bloomStore[key]
        else:
            bl=createNewBloomFilter(maxElement)
        bl.add(value)
        bloomStore[key]=bl

def removeKeyBloom(key):
    '''
    清除指定布隆运算
    :param key:
    :return:
    '''
    with lock:
        if key in bloomStore:
            del bloomStore[key]

def save():
    global bloomStore
    pickle_file = open('bloomStore.pkl', 'wb')  ##注意打开方式一定要二进制形式打开
    pickle.dump(bloomStore, pickle_file)  ##把列表永久保存到文件中
    pickle_file.close()  ##关闭文件

def load():
    global bloomStore
    pkl_file = open('bloomStore.pkl','rb')    ## 以二进制方式打开文件
    bloomStore=pickle.load(pkl_file)          ##用load（）方法把文件内容序列化为Python对象
    pkl_file.close()

def schedule():
    '''
    数据十分钟保存一次
    :return:
    '''
    while True:
        time.sleep(1 * 60)
        try:
            save()
            print("保存一次")
        except Exception:
            pass

threading.Thread(target=schedule, name='schedule').start()
if os.path.exists("bloomStore.pkl"):
    print("文件存在加载")
    load()

if __name__=="__main__":
    application.run(host='0.0.0.0', port=8080, debug=False, use_reloader=False)