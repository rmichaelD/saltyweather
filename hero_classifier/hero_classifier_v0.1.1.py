#TensorFlow test bed
##################################
#TO CONVERT WEMB TO FOOTER IMAGES
# ffmpeg -i webm_sources/mccree.webm -r 10 -filter:v "crop=in_w/18:in_h/10:in_w*0.07:in_h*0.81" -ss 00:00:05 mccree-%03d.png
###################################

import tensorflow as tf
import numpy as np
from PIL import Image, ImageOps
import random
import os
import time
from datetime import datetime
import requests
import pymysql
import subprocess

print("=======================================")
print("Classifier began at " + str(datetime.now()))
print("=======================================")
start = time.time()

random.seed(400)
label_names = ['genji', 'mccree', 'pharah', 'reaper', 'soldier76', 'tracer', 'bastion', 'hanzo', 'junkrat', 'mei', 'torbjorn', 'widowmaker', 'd.va', 'reinhardt', 'roadhog', 'winston', 'zarya', 'lucio', 'mercy', 'symmetra', 'zenyatta']
label_values = np.identity(len(label_names))
labels = {name: value for name, value in zip(label_names, label_values)}
label_size = len(labels)
label_count = {name: count for name, count in zip(label_names, [0]*label_size)}
webm_nb = 10

m = 2
w, h = 32*m, 32*m
image_size = w*h
channel_nb = 3

def get_cnx_to_db():
    try:
        cnx = pymysql.connect(user="", password="", 
                                      host="localhost", unix_socket="/var/run/mysqld/mysqld.sock", 
                                      database="salty_weather", charset='utf8')
    except pymysql.Error as err:
        print(err)
    else:
        print("Connection to DB successful")
        return cnx

def weight_variable(shape):
  initial = tf.truncated_normal(shape, stddev=0.1)
  return tf.Variable(initial)

def bias_variable(shape):
  initial = tf.constant(0.1, shape=shape)
  return tf.Variable(initial)

def conv2d(x, W):
  return tf.nn.conv2d(x, W, strides=[1, 1, 1, 1], padding='SAME')

def max_pool_2x2(x):
  return tf.nn.max_pool(x, ksize=[1, 2, 2, 1],
                        strides=[1, 2, 2, 1], padding='SAME')
                        
def get_im_array(filename, w, h):
    im = Image.open(filename)
    im = ImageOps.fit(im, (w, h), Image.ANTIALIAS).convert('LA')
    im_array = im.getdata()
    white_pixel = im_array[0][1]
    tmp_array = np.zeros(image_size)
    for pixel in range(image_size):
#        if im_array[pixel][0] / white_pixel > 0.55:
#            tmp_array[pixel] = 255 / white_pixel
#        elif im_array[pixel][0] / white_pixel < 0.45:
#            tmp_array[pixel] = 0 / white_pixel
#        else:
        tmp_array[pixel] = im_array[pixel][0] / white_pixel
    return tmp_array
    
def get_pngs(name):
    command = "mkdir " + "gfycat_testing/"+name
    os.system(command)
    try:
        output = subprocess.check_output("ffmpeg -i gfycat_testing/"+name+".webm -t 3 -r 1 -vf cropdetect -f null - 2>&1 | awk '/crop/ { print $NF }' | tail -1", shell=True)
        crop = output.decode().rstrip()
        crop=crop.split('=')[1]
        crop=crop.split(':')
        w = str(crop[0])
        h = str(crop[1])
        x = str(crop[2])
        y = str(crop[3])
        command = "ffmpeg -i gfycat_testing/"+name+".webm -r 1 -filter:v \"crop="+w+"/3:"+h+"/3:"+x+":"+y+"+"+h+"\" -pix_fmt gray gfycat_testing/"+name+"/%03d-L.png" #Left
        os.system(command)
        command = "ffmpeg -i gfycat_testing/"+name+".webm -r 1 -filter:v \"crop="+w+"/3:"+h+"/3:"+x+"+"+w+"/3:"+y+"+"+h+"\" -pix_fmt gray gfycat_testing/"+name+"/%03d-M.png" #Left
        os.system(command)
        command = "ffmpeg -i gfycat_testing/"+name+".webm -r 1 -filter:v \"crop="+w+"/3:"+h+"/3:"+x+"+"+w+"*2/3:"+y+"+"+h+"\" -pix_fmt gray gfycat_testing/"+name+"/%03d-R.png" #Left
        os.system(command)
        command = "ffmpeg -i gfycat_testing/"+name+"/%03d-L.png  -vf scale=64:64 gfycat_testing/"+name+"/%03d-L.png" #Left
        os.system(command)
        command = "ffmpeg -i gfycat_testing/"+name+"/%03d-M.png  -vf scale=64:64 gfycat_testing/"+name+"/%03d-M.png" #Middle
        os.system(command)
        command = "ffmpeg -i gfycat_testing/"+name+"/%03d-R.png  -vf scale=64:64 gfycat_testing/"+name+"/%03d-R.png" #Right
        os.system(command)
    except:
        return
    
def clean_folder(name):
    command = "rm -r gfycat_testing/"+name+"*"
    os.system(command)
    
def download_webm(gfycat_id):
    webm_link = "https://giant.gfycat.com/"+gfycat_id+".webm"
    
    print("Downloading: "+webm_link)
    r = requests.get(webm_link, stream = True)
    with open("gfycat_testing/"+gfycat_id+".webm", 'wb') as f:
        for chunk in r:
            f.write(chunk)
            
def is_hero(db_key, gfycat_id):
    cursor = cnx.cursor()
    find_link = "SELECT COUNT(1) FROM overwatch_highlights WHERE id = %s AND hero IS NOT NULL"
    cursor.execute(find_link, db_key)
    return cursor.fetchone()[0]
            
def get_gfycat_ids(cnx):
    date = str(datetime.now().date())
    cursor = cnx.cursor(pymysql.cursors.DictCursor)
    query = """
            SELECT id, url
            FROM overwatch_highlights
            WHERE DATE(date) = %s
            ORDER BY date DESC
            """
    data = (date)
    cursor.execute(query, data)
    gfycat_ids = []
    for row in cursor.fetchall():
        gfycat_ids.append((row['id'], row['url'].split('/')[3]))
    return gfycat_ids
    
def add_hero(db_key, hero, confidence):
    cursor = cnx.cursor()
    add_data = """UPDATE overwatch_highlights
                SET hero = %s, confidence = %s
                WHERE id = %s"""
    data = (hero, confidence, db_key)
    cursor.execute(add_data, data)
    try:
        cnx.commit()
    except:
        print("-Commit has failed")       
    cursor.close()
        
x = tf.placeholder(tf.float32, [None, image_size, channel_nb])
x_image = tf.reshape(x, [-1,h,w,channel_nb])
y_ = tf.placeholder(tf.float32, [None, label_size])

#1st conv layer
W_conv1 = weight_variable([5, 5, channel_nb, 32]) #5x5 conv window, 3 colour channels, 32 outputted feature maps
b_conv1 = bias_variable([32])
h_conv1 = tf.nn.relu(conv2d(x_image, W_conv1) + b_conv1)
h_pool1 = max_pool_2x2(h_conv1)

#2nd conv layer
W_conv2 = weight_variable([5, 5, 32, 64])
b_conv2 = bias_variable([64])
h_conv2 = tf.nn.relu(conv2d(h_pool1, W_conv2) + b_conv2)
h_pool2 = max_pool_2x2(h_conv2)

#1st fully connected layer
W_fc1 = weight_variable([int(h/4) * int(w/4) * 64, 1024])
b_fc1 = bias_variable([1024])
h_pool2_flat = tf.reshape(h_pool2, [-1, int(h/4) * int(w/4) * 64])
h_fc1 = tf.nn.relu(tf.matmul(h_pool2_flat, W_fc1) + b_fc1)

##2st fully connected layer
#W_fc1 = weight_variable([int(h/4) * int(w/4) * 64, 1024])
#b_fc1 = bias_variable([1024])
#h_pool2_flat = tf.reshape(h_pool2, [-1, int(h/4) * int(w/4) * 64])
#h_fc1 = tf.nn.relu(tf.matmul(h_pool2_flat, W_fc1) + b_fc1)

#droupout
keep_prob = tf.placeholder(tf.float32)
h_fc1_drop = tf.nn.dropout(h_fc1, keep_prob)

#softmax output layer
W_fc2 = weight_variable([1024, label_size])
b_fc2 = bias_variable([label_size])
logits = tf.matmul(h_fc1_drop, W_fc2) + b_fc2
y_conv = tf.nn.softmax(logits)

cross_entropy = tf.nn.softmax_cross_entropy_with_logits(logits, y_)
train_step = tf.train.AdamOptimizer(1e-4).minimize(cross_entropy)
correct_prediction = tf.equal(tf.argmax(y_conv,1), tf.argmax(y_,1))
accuracy = tf.reduce_mean(tf.cast(correct_prediction, tf.float32))

init = tf.initialize_all_variables()
sess = tf.Session()
sess.run(init)

saver = tf.train.Saver()
saver.restore(sess, "/home/roger/Salty_Weather/Overwatch/Highlights_Archive/64x64.ckpt_low_f-10000")

try:
    cnx = get_cnx_to_db()
    gfycat_ids = get_gfycat_ids(cnx)
    for db_key, name in gfycat_ids:
        if is_hero(db_key, name):
            print(name+" already present or name invalide")
        else:
            download_webm(name)
            get_pngs(name)
            batch_xs = []
            frame_count = int(len(os.listdir("gfycat_testing/"+name))/3)
            if frame_count > 0:
                for i in range(frame_count):
                    image_left = "gfycat_testing/"+name+"/"+str(i+1).zfill(3)+"-L.png"
                    L = get_im_array(image_left, w, h)
                    image_middle = "gfycat_testing/"+name+"/"+str(i+1).zfill(3)+"-M.png"
                    M = get_im_array(image_middle, w, h)
                    image_right = "gfycat_testing/"+name+"/"+str(i+1).zfill(3)+"-R.png"
                    R = get_im_array(image_right, w, h)
                    batch_xs.append(np.transpose([L, M, R]))
                ys = sess.run(y_conv, feed_dict={x: batch_xs, keep_prob: 1.0})
                ys_max_index = np.argmax(ys, axis = 1)
                freq_max_index = np.argmax(np.bincount(ys_max_index))
                hero = label_names[freq_max_index]
                confidence = float(np.mean(ys, axis=0)[freq_max_index])
                add_hero(db_key, hero, confidence)
            else:
                print("No files found for "+name)
            #clean_folder(name)
    end = time.time()
    print("=======================================")
    print("Testing ended at " + str(datetime.now()) + " and last for " + str(end - start) + " secondes")
    print("=======================================")
    
    time.sleep(300)
finally:
    cnx.close()
