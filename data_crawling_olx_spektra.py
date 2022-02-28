import requests 
from bs4 import BeautifulSoup
import pandas as pd
import unicodedata
import re
from datetime import datetime, timedelta
from math import ceil
import time
from time import sleep
import cx_Oracle
import sys

i=0
count=0

now = datetime.now()
dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
start_time=time.time()

s = requests.Session()
s.headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36'

def get_text_attr (page, tag, data_aut_id, kelas):
    soup= BeautifulSoup(page.text, 'lxml')
    attr = soup.find(tag, {"data-aut-id":data_aut_id, "class":kelas})
    attr = attr.get_text() if attr else u""
    attr = unicodedata.normalize('NFKD', attr).encode('ascii','ignore')
    attr = attr.decode("utf-8")
    
    return attr

def detail_produk (id_prod, batch, page, link_iklan):
    soup = BeautifulSoup(page.text, 'lxml')
    merk = get_text_attr (page, "span", "value_make", "_2vNpt")
    kondisi = get_text_attr (page, "span", "value_condition", "_2vNpt")
    deskripsi_lengkap = get_text_attr (page, "div", "itemDescriptionContent", "")
    deskripsi_singkat = get_text_attr (page, "h1", "itemTitle", "_3rJ6e")
    nama_toko = get_text_attr (page, "div", "", "_3oOe9")
    
    id_penjual = get_text_attr (page, "div", "", "_1oSdP")
    id_penjual = id_penjual.split("/")
    id_penjual = id_penjual[-1]
    
    harga = get_text_attr (page, "span", "itemPrice", "_2xKfz")
    harga = (harga.replace('Rp','').replace(',','').replace(' ','').replace('.',''))
    
    location = get_text_attr (page, "span", "", "_2FRXm")
    
    if location == '':
        return
        
    location = location.split(", ")
    # print(location)
    kecamatan_p = location[0]
    kota_p = location[1]
    provinsi_p = location[2]
    
    date = get_text_attr (page, "div", "itemCreationDate", "_2DGqt")
    list_date = date.split(" ")
    if date=="Hari ini":
        date = datetime.today().strftime('%d %b')
    elif date=="Kemarin":
        kemarin=datetime.today()-timedelta(days=1)
        date=kemarin.strftime('%d %b')
    elif list_date[1]=="hari":
        jml=int(list_date[0])
        kemarin=datetime.today()-timedelta(days=jml)
        date=kemarin.strftime('%d %b')
    
    anggota = soup.find_all("div","rui-u2K6U rui-2p-vC rui-38RAu rui-1O2Hi")
    anggota = anggota[0].find("span").find("span").get_text() if anggota else u''
    anggota = unicodedata.normalize('NFKD', anggota).encode('ascii','ignore')
    anggota = anggota.decode("utf-8")
    print ('deskripsi penjual: '+anggota)
    list_anggota = anggota.split(" ")
    if anggota=="Hari ini":
        anggota = datetime.today().strftime('%b %y')
    elif anggota=="Kemarin":
        kemarin=datetime.today()-timedelta(days=1)
        anggota=kemarin.strftime('%b %y')
    elif list_anggota[1]=="hari":
        jml=int(list_anggota[0])
        kemarin=datetime.today()-timedelta(days=jml)
        anggota=kemarin.strftime('%b %y')
    
    id_penjual = soup.find("div","_1oSdP")
    id_penjual = id_penjual.find("a").get("href") if id_penjual else ''
    id_penjual = id_penjual.split("/")
    id_penjual = id_penjual[-1]
    
    conn_str = 'narsys/narsys@10.17.18.25:1521/fifnartest'
    conn = cx_Oracle.connect(conn_str)
    cur = conn.cursor()
    
    statement = "INSERT INTO NARSYS.stg_data_crawling_spektra (ID_IKLAN, ID_PENJUAL, NAMA_TOKO, LINK_IKLAN, KECAMATAN, KOTA, PROPINSI, TGL_IKLAN, MEREK, KONDISI, HARGA, DESKRIPSI_SINGKAT, DESKRIPSI_LENGKAP, DESKRIPSI_PENJUAL, TANGGAL_CRAWLING, SOURCE) VALUES (:id_iklan, :id_penjual, :nama_toko, :link_iklan, :kecamatan, :kota, :propinsi, :tgl_iklan, :merek, :kondisi, :harga, :deskripsi_singkat, :deskripsi_lengkap, :deskripsi_penjual, SYSDATE, :source)"
    cur.execute(statement, id_iklan=id_prod, id_penjual=id_penjual, nama_toko=nama_toko, link_iklan=link_iklan, kecamatan=kecamatan_p, kota=kota_p, propinsi=provinsi_p, tgl_iklan=date, merek=merk, kondisi=kondisi, harga=harga, deskripsi_singkat=deskripsi_singkat, deskripsi_lengkap=deskripsi_lengkap, deskripsi_penjual=anggota, source="OLX")
    conn.commit()
    
    conn.close()

def cek_produk(id_prod,link, batch):
    sleep(10)
    
    page = s.get(link)
	
    if page.status_code == requests.codes.ok:
        detail_produk (id_prod, batch, page, link)
        

def get_attr_data(p, batch):
    global count
    count +=1
    print (count)
    
    link = p.find("a").get('href')
    list_link = link.split("-")
    id_prod = list_link[-1]
    link = "https://www.olx.co.id"+link
    print ("Link: "+link)
    
    conn_str = 'schema/password@10.17.18.25:1521/fifnartest'
    conn = cx_Oracle.connect(conn_str)
    cur = conn.cursor()
    
    cek = "SELECT ID_IKLAN FROM NARSYS.stg_data_crawling_spektra WHERE ID_IKLAN = :id_iklan"
    cur.execute(cek, id_iklan=id_prod)
    result = cur.fetchall()
    conn.close()
    
    if result == []:                
        cek_produk(id_prod, link, batch)
        
def parse_page(url, batch):
    sleep(10)
    global i
    page = s.get(url)
    print (page.status_code)
    if page.status_code == requests.codes.ok:
        soup = BeautifulSoup(page.text, 'lxml')
        list_produk = soup.find_all("li", "EIR5N")
        
        for p in list_produk:
            get_attr_data(p, batch)
            
def loop_page(url, batch, total_batch):
    page = s.get(url)
    if page.status_code == requests.codes.ok:
        soup = BeautifulSoup(page.text, 'lxml')
        jml_iklan = soup.find("p","FYmzo")
        jml_iklan = jml_iklan.get_text() if jml_iklan else b''
        unicodedata.normalize('NFKD', jml_iklan).encode('ascii','ignore')
        
        if jml_iklan != 'a':
            jml_iklan = jml_iklan.split(' ')
            jml_iklan = float(jml_iklan[0].replace('.',''))
        else:
            jml_iklan = 150
            
        ulang = int(ceil(jml_iklan/15))        
        n = str(ulang)
        last_digit = int(n[len(n)-1])
        plus = 10 - last_digit
        total_perulangan = ulang + plus
        bb = total_perulangan
        ba = total_perulangan
        selisih = int(total_perulangan/total_batch)
        arrbb=[]
        arrba=[]
        
        for n in range (total_batch-1, -1, -1):
            bb = bb - selisih
            arrbb.insert(n,bb)
            arrba.insert(n,ba)
            ba = ba - selisih
    
        print (total_perulangan)
        print (jml_iklan)
        for u in range(arrbb[batch-1], arrba[batch-1]):
            # print (u)
            next_page_partial = "&page="+str(u)
            next_page = url + next_page_partial
            # print ('Next Page: '+next_page)
            parse_page(next_page, batch)
            
if __name__ == '__main__':
    batch = int(sys.argv[1])
    total_batch = int(sys.argv[2])
    list_link  = str(sys.argv[3])
    
    # list_link = pd.read_csv('D:/Nova/Nasional.txt', header = None)
    # list_link = ['https://www.olx.co.id/handphone_c208?filter=condition_eq_baru%2Cmake_eq_elektronik-gadget-handphone-samsung', 'https://www.olx.co.id/handphone_c208?filter=condition_eq_baru%2Cmake_eq_elektronik-gadget-handphone-xiaomi']
    # print(range(len(list_link)))
    # for i in range(len(list_link)):
    base_url = list_link
    loop_page(base_url, batch, total_batch)
    
    print ("Run Date: "+ dt_string)
    print("Running time: %s minutes" % (round(((time.time() - start_time)/60),2)))
