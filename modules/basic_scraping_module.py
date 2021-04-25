import requests
#from fake_useragent import UserAgent
from bs4 import BeautifulSoup as bs
#import os

def get_response(url, is_cache=False):
    #ua = UserAgent()
    #print(ua.random)
    #x = ua.random
    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2272.96 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    }
    #'From': 'YOUR EMAIL ADDRESS'
    #headers = {"User-Agent": "Googlebot",}
    if is_cache == True:
        url = f"http://webcache.googleusercontent.com/search?q=cache:{url}"
    r = requests.get(url, headers=headers, allow_redirects=False)
    if r.status_code == requests.codes.ok: 
        r.encoding = "utf-8-sig"
        #r.encoding = "utf-8"
        return r
    else:
        print("Fail to get response.")
        return None

def get_soup(response):
    return bs(response.text, "lxml")

"""
def download_pic(merchandise_name, image, No):
    pic_url = f"https://cf.shopee.tw/file/{image}_tn"
    r = get_response(pic_url)
    if r != None:        
        mdsePics_dir = f"{merchandise_name}_商品圖片"
        if not os.path.exists(mdsePics_dir):
            os.mkdir(mdsePics_dir)
        try:
            mdsePic_path = f"{mdsePics_dir}/{No}.jfif"
            with open(mdsePic_path, "wb") as fp:
                fp.write(r.content)
            print(f"第 {No} 件商品獲取圖片成功！")
        except:
            print(f"第 {No} 件商品獲取圖片失敗 (Fail to store image into the merchandise's directory)")
    else:
        print(f"第 {No} 件商品獲取圖片失敗 (Fail to get request)")
"""

#r = get_response("https://www.google.com")
#print(r.content)