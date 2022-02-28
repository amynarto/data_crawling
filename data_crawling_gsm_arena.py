import requests 
from bs4 import BeautifulSoup
import pandas as pd
import unicodedata
import re
from datetime import datetime, timedelta
from math import ceil
import time
from time import sleep
import sys
from requests.auth import HTTPProxyAuth
import cx_Oracle

now = datetime.now()
dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
start_time=time.time()
# headers = { 'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0' }
s = requests.Session()
s.headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36'

def get_link_product(full_link, base_link):
    page = s.get(full_link, allow_redirects=False)
    soup= BeautifulSoup(page.text, 'lxml')
    
    li = soup.find("div","makers")
    for a in li.findChildren("a", href=True):
        if a.text:
            link_product = base_link + a['href']
            print (link_product)
            
            conn_str = 'narsys/narsys@10.17.18.25:1521/fifnartest'
            conn = cx_Oracle.connect(conn_str)
            cur = conn.cursor()
            
            cek = "SELECT PRODUCT FROM NARSYS.stg_data_crawling_gsm_arena WHERE LINK_PRODUCT = :link_product"
            cur.execute(cek, link_product=link_product)
            result = cur.fetchall()
            conn.close()
            
            if result == []:            
                crawling_data (link_product)
                sleep(10)

def get_text_attr (page, data_spec):
    soup= BeautifulSoup(page.text, 'lxml')
    attr = soup.find("td", {"data-spec":data_spec})
    attr = attr.get_text() if attr else u""
    attr = unicodedata.normalize('NFKD', attr).encode('ascii','ignore')
    attr = attr.decode("utf-8")
    
    return attr

def crawling_data (link_product):
    page = s.get(link_product, allow_redirects=False)
    soup= BeautifulSoup(page.text, 'lxml')
    
    merk = soup.find("h1","specs-phone-name-title")
    merk = merk.get_text() if merk else u''
    merk = unicodedata.normalize('NFKD', merk).encode('ascii', 'ignore')
    
    network = soup.find("a","link-network-detail collapse")
    network = network.get_text() if network else u''
    network = unicodedata.normalize('NFKD', network).encode('ascii', 'ignore')
    
    launch_announced = get_text_attr (page,"year")
    launch_status = get_text_attr (page,"status")
    
    body_dimensions = get_text_attr (page,"dimensions")
    body_weight = get_text_attr (page,"weight")
    body_sim = get_text_attr (page,"sim")
    
    display_type = get_text_attr (page,"displaytype")
    display_size = get_text_attr (page,"displaysize")
    display_resolution = get_text_attr (page,"displayresolution")
    display_protection = get_text_attr (page,"displayprotection")
    
    platform_os = get_text_attr (page,"os")
    platform_chipset = get_text_attr (page,"chipset")
    platform_cpu = get_text_attr (page,"cpu")
    platform_gpu = get_text_attr (page,"gpu")
    
    memory_slot = get_text_attr (page, "memoryslot")
    memory_internal = get_text_attr (page, "internalmemory")
    memory_other = get_text_attr (page, "memoryother")
    
    camera = get_text_attr (page, "cam1modules")
    camera = camera.replace("\r\n", " | ")
    camera_features = get_text_attr (page, "cam1features")
    camera_video = get_text_attr (page, "cam1video")
    
    selfcam = get_text_attr (page, "cam2modules")
    selfcam = selfcam.replace("\r\n", " | ")
    selfcam_features = get_text_attr (page, "cam2features")
    selfcam_video = get_text_attr (page, "cam2video")
    
    sound = soup.findAll("table")[8].findAll("td", "nfo")

    sound_loud = sound[0]
    sound_loud = sound_loud.get_text() if sound_loud else u''
    sound_loud = unicodedata.normalize('NFKD', sound_loud).encode('ascii', 'ignore')
    
    sound_jack = sound[1]
    sound_jack = sound_jack.get_text() if sound_jack else u''
    sound_jack = unicodedata.normalize('NFKD', sound_jack).encode('ascii', 'ignore')
    
    comms_wlan = get_text_attr (page, "wlan")
    comms_bluetooth = get_text_attr (page, "bluetooth")
    comms_gps = get_text_attr (page, "gps")
    comms_nfc = get_text_attr (page, "nfc")
    comms_radio = get_text_attr (page, "radio")
    comms_usb = get_text_attr (page, "usb")
    
    features_sensor = get_text_attr (page, "sensors")
    
    battery_type = get_text_attr (page, "batdescription1")
    
    misc_colors = get_text_attr (page, "colors")
    misc_models = get_text_attr (page, "models")
    misc_price = get_text_attr (page, "price")
    
    conn_str = 'narsys/narsys@10.17.18.25:1521/fifnartest'
    conn = cx_Oracle.connect(conn_str)
    cur = conn.cursor()
        
    statement = "INSERT INTO NARSYS.stg_data_crawling_gsm_arena (LINK_PRODUCT, PRODUCT, NETWORK_TECHNOLOGY, LAUNCH_ANNOUNCED, LAUNCH_STATUS, BODY_DIMENSION, BODY_WEIGHT, BODY_SIM, DISPLAY_TYPE, DISPLAY_SIZE, DISPLAY_RESOLUTION, DISPLAY_PROTECTION, PLATFORM_OS, PLATFORM_CHIPSET, PLATFORM_CPU, PLATFORM_GPU, MEMORY_SLOT, MEMORY_INTERNAL, MEMORY_OTHER, CAMERA, CAMERA_FEATURES, CAMERA_VIDEO, SELFCAM, SELFCAM_FEATURES, SELFCAM_VIDEO, SOUND_LOUD, SOUND_JACK, COMMS_WLAN, COMMS_BLUETOOTH, COMMS_GPS, COMMS_NFC, COMMS_RADIO, COMMS_USB, FEATURES_SENSOR, BATTERY_TYPE, MISC_COLORS, MISC_MODELS, MISC_PRICE, CREATED_DATE) VALUES (:link, :merk, :network, :launch_announced, :launch_status, :body_dimensions, :body_weight, :body_sim, :display_type, :display_size, :display_resolution, :display_protection, :platform_os, :platform_chipset, :platform_cpu, :platform_gpu, :memory_slot, :memory_internal, :memory_other, :camera, :camera_features, :camera_video, :selfcam, :selfcam_features, :selfcam_video, :sound_loud, :sound_jack, :comms_wlan, :comms_bluetooth, :comms_gps, :comms_nfc, :comms_radio, :comms_usb, :features_sensor, :battery_type, :misc_color, :misc_models, :misc_price, sysdate)"
    cur.execute(statement, link=link_product, merk=merk, network=network, launch_announced=launch_announced, launch_status=launch_status, body_dimensions=body_dimensions, body_weight=body_weight, body_sim=body_sim, display_type=display_type, display_size=display_size, display_resolution=display_resolution, display_protection=display_protection, platform_os=platform_os, platform_chipset=platform_chipset, platform_cpu=platform_cpu, platform_gpu=platform_gpu, memory_slot=memory_slot, memory_internal=memory_internal, memory_other=memory_other, camera=camera, camera_features=camera_features, camera_video=camera_video, selfcam=selfcam, selfcam_features=selfcam_features, selfcam_video=selfcam_video, sound_loud=sound_loud, sound_jack=sound_jack, comms_wlan=comms_wlan, comms_bluetooth=comms_bluetooth, comms_gps=comms_gps, comms_nfc=comms_nfc, comms_radio=comms_radio, comms_usb=comms_usb, features_sensor=features_sensor, battery_type=battery_type, misc_color=misc_colors, misc_models=misc_models, misc_price=misc_price)
    conn.commit()
        
    conn.close()

def get_another_page(full_link, base_link):
    page = s.get(full_link, allow_redirects=False)
    soup= BeautifulSoup(page.text, 'lxml')
    link_page = soup.find("div", "nav-pages")
    
    for a in link_page.findChildren("a", href=True):
        if a.text:
            full_link_product = base_link + a['href']
            get_link_product(full_link_product, base_link)

if __name__ == '__main__':
    base_link = 'https://www.gsmarena.com/'
    # link = ['oppo-phones-82.php', 'vivo-phones-98.php', 'samsung-phones-9.php', 'apple-phones-48.php', 'xiaomi-phones-80.php', 'realme-phones-118.php']
    link = str(sys.argv[1])
    
    full_link = base_link + link
    get_link_product(full_link, base_link)
    get_another_page(full_link, base_link)
    
    # for a in range(len(link)):
    #     full_link = base_link + link[a]
    #     get_link_product(full_link, base_link)
    #     get_another_page(full_link, base_link)
        
    print ("Run Date: "+ dt_string)
    print("Running time: %s minutes" % (round(((time.time() - start_time)/60),2)))