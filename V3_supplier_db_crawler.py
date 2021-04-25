from selenium import webdriver as wd
from selenium.webdriver.chrome.options import Options
import time
import csv
import os
import random
import json
import shutil
import pandas as pd
from modules.checker import Checker
from modules.basic_scraping_module import get_response #, get_soup
from modules.supplier_utils.uniform_category_transformer import query_uniform_category

def read_scrapy_setting():
    img_hist = "./res3/img_html_source/img_hist.txt"
    with open(img_hist, "r", encoding="utf-8-sig") as fp:
        data = fp.readlines()
    break_point = int(data[1].split(":")[-1].strip())
    avg_wait_time = int(data[2].split(":")[-1].strip())
    return break_point, avg_wait_time
    
class Webdriver():
    def get_webdriver(self):
        chrome_options = Options()
        chrome_options.headless = True
        wd_path = "D:/geckodriver/chromedriver.exe"
        driver = wd.Chrome(wd_path, options=chrome_options)
        driver.implicitly_wait(10)
        return driver

class Clothes_crawler():
    def imgID_padding(self):
        csv_path = "./res3/tier_2.csv"
        df = pd.read_csv(csv_path)
        #print(data.head())
        new_col_data = [i for i in range(1, len(df)+1)]
        new_col_name = "img_id"
        df[new_col_name] = new_col_data   
        #print(data.tail())
        out_csv_path = "./res3/tier_2_modified.csv"
        df.to_csv(out_csv_path, encoding="utf-8-sig", index=False)
    ###########################################################
    def copy_single_prod_img(self, img_id, existing_img_id):
        img_dir = "./res3/img_html_source/"
        shutil.copy(f"{img_dir}{existing_img_id}.jpg", f"{img_dir}{img_id}.jpg")
    
    def download_single_prod_img(self, prod_img_link, img_id, wait_time):
        img_path = f"./res3/img_html_source/{img_id}.jpg"
        if os.path.exists(img_path):
            print(f"[img {img_id}] Image is already exists.")
            return 0
        
        # [***] send requests to image link
        # put all correct image links to the new csv file
        # path: ./res3/img_html_source
        if "grey.gif" not in prod_img_link:
            try:
                r = get_response(prod_img_link)
                with open(img_path, "wb") as fp:
                    fp.write(r.content)
                print(f"[img {img_id}] Successfully downloaded.")
                # 等待隨機時間 (以傳入參數 wait_time 為中心)
                self.wait_some_seconds(wait_time + random.randint(-53,41)/10)
                return 1
            except:
                print(f"[img {img_id}] ERR-2: Fail to access image link when scrapying image")
                return -1
        else:
            print("跳過")
    def wait_some_seconds(self, wait_time):
        #print(f"(隨機)等待 {wait_time} 秒")
        print(f"等待 {wait_time} 秒")
        time.sleep(wait_time)
        
    def download_multiple_prod_imgs(self, break_point=-1, wait_time=10):
        # reset crawler
        self.set_driver()
        
        # read image history if exists
        img_hist = "./res3/img_html_source/img_hist.txt"
        if os.path.exists(img_hist):
            with open(img_hist, "r", encoding="utf-8-sig") as fp:
                data = fp.readlines()
            img_id_start = int(data[0].split(":")[-1].strip()) # starts from next image of last image in the directory
        else:
            img_id_start = 5001 # 1
        
        # read image mapping if exists
        img_mapping_json = "./res3/img_html_source/img_record.json"
        if os.path.exists(img_mapping_json):
            with open(img_mapping_json, "r", encoding="utf-8-sig") as fp:
                img_mapping = json.load(fp)
        else:
            img_mapping = dict() # k: prod_link, v: img_id
        
        # create env
        env_path = r"./res3/img_html_source"
        if not os.path.exists(env_path):
            os.mkdir(env_path)
        
        # read product urls from existing tier-2 csv
        csv_path = "./res3/tier_2_modified.csv"
        prod_data = pd.read_csv(csv_path)
        #print(prod_data.tail())
        '''
        prodIDs, prod_SKU_IDs, prod_links = prod_data["productID"], prod_data["product_SKU_ID"], prod_data["product_link"] 
        '''
        prodIDs, prod_SKU_IDs, prod_img_links = prod_data["productID"], prod_data["product_SKU_ID"], prod_data["product_img_link"]
        # test
        #print(prodIDs.head())
        #print(prod_SKU_IDs.head())
        #print(prod_links.head())
        for i in range(img_id_start-1, len(prodIDs)): # i starts from 0
            prod_img_link = prod_img_links[i]
            img_id = i+1 # integer
            if i == break_point: # break_point starts from 1
                break
            
            print("\n", f"No: {img_id}", sep="")
            print(f"prodID: {prodIDs[i]}")
            print(f"prod_SKU_ID: {prod_SKU_IDs[i]}")
            print(f"prod_img_link: {prod_img_link}")
            
            #if prod_link not in img_mapping.keys():
            if not os.path.exists(f"{env_path}/{img_id}.jpg"):
                img_mapping[prod_img_link] = img_id
                ''' 到server圖檔資料庫抓圖片 '''
                print(f"[img {img_id}] 圖片不存在，正在抓圖片")
                return_val = self.download_single_prod_img(prod_img_link, img_id, wait_time)
                if return_val == -1:
                    break
            else:
                ''' 複製已存圖片 '''
                print(f"[img {img_id}] 相同圖片已存在本機，正在複製圖片")
                existing_img_id = img_mapping[prod_img_link]
                self.copy_single_prod_img(img_id, existing_img_id)
            
        #print("img_mapping:", img_mapping, sep="\n")
        
        # 紀錄 img_id
        with open(img_hist, "r", encoding="utf-8-sig") as fp:
            data = fp.readlines()
        msg = ""
        msg += data[0].split(":")[0] + ": " + str(img_id) + "\n" # 更新開始索引
        msg += data[1].split(":")[0] + ": " + "\n" # 清空結束索引
        msg += data[2]
        '''
        with open(img_hist, "w", encoding="utf-8-sig") as fp:
            fp.write(str(img_id))
        '''
        with open(img_hist, "w", encoding="utf-8-sig") as fp:
            fp.write(msg)
        
        # 紀錄 img_mapping
        with open(img_mapping_json, "w", encoding="utf-8-sig") as fp:
            json.dump(img_mapping, fp, ensure_ascii=False)
        
    def set_driver(self):
        webdriver = Webdriver()
        self.driver = webdriver.get_webdriver()
        
    def get_genres(self):
        return ["WOMEN","MEN","KIDS","BABY","SPORTS"]
    
    def scroll(self):
        # 下拉至頁尾以fetch所有品項
        for i in range(4):
            self.driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")
            time.sleep(1)
    
    def save_to_csv(self, list_obj, csv_path, col_names):
        if not os.path.exists("./res3"):
            os.mkdir("./res3")         
        
        record_amount = 0 # in case: csv file isn't exists
        if os.path.exists(csv_path):
            with open(csv_path, mode='r', encoding="utf-8-sig") as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=',')
                # 包含header列的總記錄筆數:
                record_amount = len([record for record in csv_reader])
                
        with open(csv_path, mode='a', newline="", encoding="utf-8-sig") as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=col_names)
            if record_amount == 0: # 該csv檔沒有header
                writer.writeheader()
            for dict_obj in list_obj:
                writer.writerow(dict_obj)
        print("csv檔案儲存完畢！")
    
    """ Clothes Website: Lativ, Tier-2 Scrapying """
    def detailPage_links_crawling(self, page_n): # 銷售標籤頁: 第 n 頁 / 190 頁
        try:
            self.set_driver()
            # 先去讀取存好的 tier_1.csv 資料
            path = "./res3/tier_1.csv"
            #print(os.path.exists(path))
            self.lativ_labels = pd.read_csv(path, header=0)
            sales_category_link = self.lativ_labels["link"] # in first, scrapying the label_page 
            #print(sales_category_link)
            data_amount = len(sales_category_link)
            print(f"共有 {data_amount} 個銷售分頁")
            #####################
            ''' 最重要的info '''
            prod_info_list = list() # [{商品ID,商品鏈結,商品價格,商品圖片,...},{.......}]
            child_category_list = list() # 不重複的款式名稱
            #####################
            xpaths = dict()
            xpaths.setdefault("child_categories", "//div[@class='child-category-name is-style']")
            xpaths.setdefault("productPage_links", "//td/div[contains(text(),'@@@')]/following-sibling::ul[1]/li[contains(@style,'margin-right')]/a")
            xpaths.setdefault("SKU_ID", "//li/a[contains(@href,'!!!')]/following-sibling::div[contains(@class,'product-color-list')]/a")
            #####################
            print("開始爬蟲...") # 開始爬各個標籤分頁
            
            # 爬取第n頁
            sales_categoryID = page_n
            link = list(sales_category_link)[page_n-1]
            
            print(f"開始搜尋第 {sales_categoryID} 個銷售分頁 ...")
            print(f"網址: {link}")
            self.driver.implicitly_wait(10)
            self.driver.get(link) # 某一個銷售分頁
            self.scroll()
            
            # 先撈出所有 "款式名稱(child categories name) (如:圓領上衣,連帽上衣的「文字」)
            tags = self.driver.find_elements_by_xpath(xpaths["child_categories"])
            child_category_names = [tag.text.strip() for tag in tags]
            print(f"共有 {len(child_category_names)} 種服飾款式")
            
            path = "./res3/child_categories.csv"
            # 遍歷所有款式名稱，抓出每一項該款式名稱下的商品資訊
            for i, child_category in enumerate(list(child_category_names)):
            #for i, child_category in enumerate(list(child_category_names)[:3]):
                print(f"正在抓第 {i+1} 種服飾款式:{child_category}")
                ''' 求算 child_categoryID '''
                need_to_append = False
                if not os.path.exists(path): # 第一次執行
                    if child_category not in child_category_list:
                        need_to_append = True
                else:
                    child_categories = pd.read_csv(path, header=0)
                    if not any(child_categories["child_category"]==child_category):
                        [child_category_list.append(-1) for _ in range(len(child_categories["child_categoryID"]))]
                        need_to_append = True
                
                if need_to_append:
                    child_category_list.append(child_category)
                
                ''' 撈出: 該款式所有衣服「商品links」'''
                xpath_link = xpaths["productPage_links"].replace("@@@", child_category)
                tags = self.driver.find_elements_by_xpath(xpath_link)
                product_links = [tag.get_attribute("href") for tag in tags]
                
                ''' 撈出: 該款式所有衣服「商品ID」'''
                productIDs = [url.split("/")[-1] for url in product_links]
                
                ''' 撈出: 該款式所有衣服「商品SKU_ID」'''
                product_SKU_IDs = dict()
                for productID in productIDs:
                    xpath = xpaths["SKU_ID"].replace("!!!", productID)
                    tags = self.driver.find_elements_by_xpath(xpath)
                    prod_SKU_links = [tag.get_attribute("href").split("/")[-1] for tag in tags]
                    product_SKU_IDs.setdefault(productID, prod_SKU_links)
                
                ''' 撈出: 該款式所有衣服「商品價格」'''
                xpath2 = xpath_link + "/following-sibling::span"
                tags = self.driver.find_elements_by_xpath(xpath2)
                product_prices = [tag.text.strip() for tag in tags]
                
                ''' 撈出: 該款式所有衣服商品「圖片網址」 ''' 
                xpath3 = xpath_link + "/img"
                
                tags = self.driver.find_elements_by_xpath(xpath3)
                product_img_links = [tag.get_attribute("src") for tag in tags]
                
                ''' 撈出: 該款式所有衣服「商品名稱」'''
                xpath4 = xpath_link + "/following-sibling::div[@class='productname']"
                tags = self.driver.find_elements_by_xpath(xpath4)
                product_names = [tag.text.strip() for tag in tags]
                
                ''' 暫存商品資訊 '''
                for i in range(len(productIDs)):
                    productID = productIDs[i]
                    # 找到該商品所有SKU_ID
                    product_SKU_ID_list = product_SKU_IDs[productID]
                    for j in range(len(product_SKU_ID_list)):
                        product_SKU_ID = product_SKU_ID_list[j]
                        prod_info_list.append({"productID": productID,
                                               "product_SKU_ID": product_SKU_ID,
                                               "product_name": product_names[i],
                                               "product_price": product_prices[i],
                                               "product_img_link": product_img_links[i],
                                               "product_link": product_links[i],
                                               "child_category": child_category,
                                               "sales_categoryID": sales_categoryID
                                               })
                        
            self.save_to_csv(prod_info_list,
                             "./res3/tier_2.csv",
                             ["productID",
                              "product_SKU_ID",
                              "product_name",
                              "product_price",
                              "product_img_link",
                              "product_link",
                              "child_category",
                              "sales_categoryID"
                             ])
            print(f"第 {sales_categoryID} 個銷售分頁爬蟲成功！")
        except:
            #print(f"第 {sales_categoryID} 個銷售分頁爬蟲失敗")
            print("為保持原子性(Atomicity)，不儲存此銷售分頁目前爬到的所有記錄")
        finally:
            self.driver.close()
            
    """ Clothes Website: Lativ, Tier-1 Scrapying """
    def labelPage_links_crawling(self):
        print("開始爬蟲...")
        self.driver.implicitly_wait(10)

        # genre_label_category => category => sales_category
        # E.g., {"WOMEN":{"上衣類":{"聯名印花長T","厚棉系列"},"襯衫類":{...},...}}
        genre_label_recorder = dict()
        #csv_saving_type = 1
        
        for genre in self.get_genres():
            print(f"正在爬 {genre} 類商品")
            url = "https://www.lativ.com.tw/{}".format(genre)
            self.driver.get(url)
            # 1. 抓出category的text
            # 2. 利用此text，找到其下的所有sales-categories
            label_recorder = dict()
            categories_text = list()
            categories = self.driver.find_elements_by_xpath("//li/h2")
            for category in categories:
                categories_text.append(category.text)
                label_recorder.setdefault(category.text, dict())
            for category_text in categories_text:
                print(f"　　正在爬 {category_text} 標籤下的銷售類別")
                xpath = f"//h2[contains(text(),'{category_text}')]" + "/../ul/li/a"
                sales_categories = self.driver.find_elements_by_xpath(xpath)
                for tag in sales_categories:
                    label_recorder[category_text].setdefault(tag.text, tag.get_attribute("href"))
            genre_label_recorder[genre] = label_recorder
        print("爬蟲結束！")
        self.driver.close()
        # 回傳爬到的所有labels
        return genre_label_recorder #, csv_saving_type
    
    def save_duplicated_SKUID_as_json():
        checker = Checker()
        path = "./res3/duplicated_SKU_IDs.json"
        duplicated_SKU_IDs = checker.check_duplicate_SKU_IDs()
        checker.save_to_json(duplicated_SKU_IDs, path)
    
    """ Clothes Website: Lativ, Tier-4 Scrapying 
        (P.S. Tier-3 is for image crawling, 
              and Tier-4 over there is for color info recrawling)
    """
    def product_scrapying(self, csv_tier_2_path, output_csv_path):
        # data-structures for providing input info
        df = pd.read_csv(csv_tier_2_path)
        SPUs, prod_SKU_links = df["product_SPU_ID"], df["product_link"]
        
        # data-structures for the verification use
        prod_names = df["product_name"]
        spu_value_counts = SPUs.value_counts()
        
        # data-structures for recording output info
        output_info = dict()
        output_info.setdefault("product_SPU_ID", list())
        output_info.setdefault("new_prod_ID", list())
        output_info.setdefault("SKU_color_name", list())
        xpaths = dict()
        xpaths.setdefault("SKU_link", "//div[@class='color']/a")
        xpaths.setdefault("SKU_img", "//div[@class='color']/a/img")
        recorded_SPUs = dict()
        recorded_SPUs.setdefault("valid", list())
        recorded_SPUs.setdefault("invalid", list())
        #n = 2
        for i, v in enumerate(zip(SPUs, prod_SKU_links)):
        #for i, v in enumerate(zip(SPUs[:n], prod_SKU_links[:n])):
            SPU, prod_link = v[0], v[1]
            if SPU not in recorded_SPUs["valid"]+recorded_SPUs["invalid"]:
                try:
                    #recorded_SPUs.append(SPU)
                    ''' Visit `prod_link` '''
                    self.set_driver()
                    self.driver.get(prod_link)
                    wait_time = 6 + random.randint(-26, 26)/10
                    self.wait_some_seconds(wait_time)
                    
                    ''' Append the current `prod_link` into one type of list in `recorded_SPUs` '''
                    # Verify the prod_link is REAL or not
                    # by extracting the product name and comparing
                    curr_prod_name = self.driver.find_element_by_xpath("//span[@class='title1']")
                    curr_prod_name = curr_prod_name.text
                    if prod_names[i] not in curr_prod_name:
                        recorded_SPUs["invalid"].append(SPU)
                        print("[WARNING] 因商品名稱與記錄不符，推測應為先前抓取時重導向到無效連結，故此輪爬蟲提早結束（進入下一輪）")
                        print(f"prod_names[{i}]: {prod_names[i]}")
                        print(f"curr_prod_name: {curr_prod_name}")
                        continue
                    
                    ''' Crawl info '''
                    SKU_links = self.driver.find_elements_by_xpath(xpaths["SKU_link"])
                    new_prod_IDs = [link.get_attribute("href").split('/')[-1] for link in SKU_links]
                    
                    # Double-verify the prod_link is REAL or not
                    # by getting the amount of all SKU products
                    # and comparing with recorded # of all SKU prods under the same SPU prods
                    if len(new_prod_IDs) != spu_value_counts[SPU]:
                        recorded_SPUs["invalid"].append(SPU)
                        print("[WARNING] 因同一SPU商品下的SKU商品數量與記錄不符，無法正確將欲爬取資訊與先前資料做連結（需手工檢查），故此輪爬蟲提早結束（進入下一輪）")
                        continue
                    else:
                        recorded_SPUs["valid"].append(SPU)
                        print(f"[INFO] 正在記錄尚未記錄的正確 SPU_ID: {SPU}")
                    
                    print("[INFO] 正在爬取 商品SKU顏色 資訊...")
                    imgs = self.driver.find_elements_by_xpath(xpaths["SKU_img"])
                    SKU_color_names = [img.get_attribute("alt") for img in imgs]
                    #tmp_SPUs = [str(SPU)] * len(imgs)
                    
                    for new_prod_ID, SKU_color_name in zip(new_prod_IDs, SKU_color_names):
                        output_info["product_SPU_ID"].append(SPU)
                        output_info["new_prod_ID"].append(new_prod_ID)
                        output_info["SKU_color_name"].append(SKU_color_name)
                    
                    '''
                    output_info["product_SPU_ID"].append(", ".join(tmp_SPUs))
                    output_info["new_prod_ID"].append(", ".join(new_prod_IDs))
                    output_info["SKU_color_name"].append(", ".join(SKU_color_names))
                    '''
                    
                    #wait_time = 3
                    #print(f"[INFO] 爬蟲結束，等待 {wait_time} 秒")
                    #time.sleep(wait_time)
                except:
                    print("[WARNING] 此輪發生未知錯誤")

        output_df = pd.DataFrame.from_dict(output_info)
        output_df.to_csv(output_csv_path,
                         index=False,
                         encoding="utf-8-sig")
    ###########################################################
    def make_dir(self, dir_path):
        if not os.path.exists(dir_path):
            print(f"[INFO] 正在建立資料夾: \"{dir_path}\"",
                  end='\n'*2)
            os.mkdir(dir_path)
        else:
            print(f"[INFO] 資料夾: \"{dir_path}\" 已存在")

    def make_dirs(self, dir_paths):
        for path in dir_paths:
            self.make_dir(path)

    def generate_download_link(self, server_id, spu_id, sku_id):    
        return f"https://s{server_id}.lativ.com.tw/i/"+\
               f"{spu_id}/{spu_id}{sku_id}1/{spu_id}{sku_id}_500.jpg"

    def prepare_empty_dirs_and_record_crawling_info(self, tier_1_csv_path, tier_2_csv_path, output_dir, tier_3_csv_path):
        ''' 建立目標路徑的上層資料夾 (media/products/) '''
        paths_to_create = list()
        tmp = output_dir.split('/')
        #MIN_IDX = 1
        #MAX_IDX = len(tmp)+1
        MIN_IDX = 2
        MAX_IDX = len(tmp)
        for i in range(MIN_IDX, MAX_IDX):
            #print(f"({i})", end=' ')
            #print('/'.join(tmp[:i]))
            paths_to_create.append('/'.join(tmp[:i]))
        #print(paths_to_create)
        self.make_dirs(paths_to_create)
        
        df1 = pd.read_csv(tier_1_csv_path)
        sales_cat_table = dict()
        genre_category_combs = set()
        for _, record in df1.iterrows():
            sales_cat_id = record["sales-category ID"]
            sales_cat_table.setdefault(sales_cat_id, dict())
            genre = record["genre"]
            uniform_category = record["uniform_category"]
            sales_cat_table[sales_cat_id]["genre"] = genre
            sales_cat_table[sales_cat_id]["uniform_category"] = uniform_category
            genre_category_combs.add(f"{output_dir}{genre}/{uniform_category}")

        # =============================================================================
        #  example: query `genre`, `category` for `sales-category ID`: 67
        # =============================================================================
        '''
        test_sales_cat_id = 67
        print(sales_cat_table[test_sales_cat_id]["genre"])
        print(sales_cat_table[test_sales_cat_id]["uniform_category"])
        '''
        
        # =============================================================================
        #  example: list all unrepeated directory
        # =============================================================================
        '''print(genre_category_combs)'''
        
        ''' 建立目標路徑的中層資料夾 (genre/category/) '''
        genre_dirs = ['/'.join(e.split('/')[:-1]) for e in genre_category_combs]
        self.make_dirs(genre_dirs)
        self.make_dirs(genre_category_combs)
        
        ''' 利用：
            (1) 輪流產生的 server_id (目標靜態伺服器主機)
            (2) spu_id
            (3) sku_id
            ※ 註: spu_id+sku_id 唯一組不重複的 SKU商品，擁有唯一商品圖片
            => 下方程式碼先藉由上述資訊，產生：
               (1) product_ID (spu_id + sku_id)
               (2) server_id (目標靜態伺服器主機)
               (3) dl_link (圖片下載鏈結)
               (4) img_path (本地圖片位置)
               (5) is_dl (是否已下載) | choices: ('Y','N')
               並產出一個 csv file: `tier_3.csv`
               使 tier_2_v??.csv 可透過 csv 文件找出每一筆
               商品紀錄對應的 `本地圖片路徑` & `線上下載網址`
        '''
        df2 = pd.read_csv(tier_2_csv_path)
        product_IDs = df2["product_ID"]
        sales_category_IDs = df2["sales_categoryID"]
        #print(sales_category_IDs[:1000])
        # =============================================================================
        #  example: get `sales_categoryID` for given `product_ID`
        # =============================================================================
        #test_product_ID = "52552___03" # expect for "80"
        #test_product_ID = "53005___01" # expect for "81"
        #print(sales_category_IDs[list(product_IDs).index(test_product_ID)])
        product_dirs = list()
        #download_links = list()
        df3_info = {"product_ID": list(),
                    "server_id": list(),
                    "dl_link":  list(),
                    "sales_cat_id":  list(),
                    "img_path":  list()}
        server_id = 0
        SERVER_NUM = 4
        for product_ID in set(product_IDs):
            spu_id, sku_id = product_ID.split("___")
            server_id += 1
            if server_id > SERVER_NUM:
                server_id = 1
            dl_link = self.generate_download_link(server_id, spu_id, sku_id)
            #download_links.append(dl_link)
            sales_cat_id = sales_category_IDs[list(product_IDs).index(product_ID)]
            
            uniform_category = sales_cat_table[sales_cat_id]["uniform_category"]
            product_dir_path = f"{output_dir}"+\
                               f"{sales_cat_table[sales_cat_id]['genre']}"+\
                               f"/{uniform_category}/{spu_id}"
            img_path = f"{product_dir_path}/{product_ID}.jpg"
            '''
            print(f"product_ID: {product_ID}")
            print(f"server_id: s{server_id}")
            print(f"dl_link: {dl_link}")
            print(f"sales_cat_id: {sales_cat_id}")
            print(f"img_path: {img_path}\n")
            '''
            df3_info["product_ID"].append(product_ID)
            df3_info["server_id"].append(f"s{server_id}")
            df3_info["dl_link"].append(dl_link)
            df3_info["sales_cat_id"].append(sales_cat_id)
            df3_info["img_path"].append(img_path)
            product_dirs.append(product_dir_path)

        df3 = pd.DataFrame(df3_info)
        df3.to_csv(tier_3_csv_path,
                   index=False,
                   encoding="utf-8-sig")

        '''
        #print(len(list(set(df2["product_ID"]))))
        #print(len(list(set(df2["product_SPU_ID"]))))
        #print(len(list(set(df2["product_SKU_ID"]))))
        #print(len(list(set(df2["product_link"]))))
        #print(len(download_links))
        
        unrepeated   spu+sku   spu      sku    prod_link    dl
        "tier_2_v2": 4267,    1560,    3296,   1560,       4267
        "tier_2_v3": 3296,    1235,      20,   1339,       3296
        '''
        
        ''' 建立目標路徑的底層資料夾 (genre/category/) '''
        self.make_dirs(product_dirs)
        #print(product_dirs)
    
    def download_single_image(self, link, img_path, wait_time):
        if not os.path.exists(img_path):
            try:
                print("[INFO] 正在下載圖片")
                r = get_response(link)
                with open(img_path, "wb") as fp:
                    fp.write(r.content)
                print("[INFO] 圖片獲取成功！\n"+\
                      f"圖片路徑:\n{img_path}")
                # 等待隨機時間 (以傳入參數 wait_time 為中心)
                self.wait_some_seconds(wait_time + random.randint(-21,21)/10)
            except:
                print("[WARNING] 無法獲取圖片")
        else:
            #print(f"[INFO] 圖片已存在 (路徑: {img_path})")
            print("[INFO] 圖片已存在")
    
    def crawl_images(self, tier_1_csv_path, tier_2_csv_path, output_dir, tier_3_csv_path):
        ''' 爬圖前置作業: 
            1. 製備階層式圖片資料夾(環境)
            2. 將圖片網址、本地路徑等資訊，記錄到 `tier_3_csv_path`
        '''
        self.prepare_empty_dirs_and_record_crawling_info(tier_1_csv_path, tier_2_csv_path, output_dir, tier_3_csv_path)
        
        '''
        爬圖片:
            1. 取得 [爬圖前置作業] 記錄在 csv file 的資訊
            2. 獲取圖片並儲存
        '''
        
        df3 = pd.read_csv(tier_3_csv_path)
        dl_links = df3["dl_link"]
        img_paths = df3["img_path"]
        wait_time = 5
        
        # =============================================================================
        #  example: download one image to `test_img_path` from `test_dl_link`
        # =============================================================================
        '''
        test_dl_link = "https://www.apowersoft.tw/wp-content/uploads/2017/07/add-ass-subtitles-to-video-logo.jpg"
        test_img_path = "D:/MyPrograms/Clothes2U/functions/台灣服飾商 ETL/Lativ_Crawler/res3/media_example/products/WOMEN/內衣類/46431___03/52202___01.jpg"
        self.download_single_image(test_dl_link, test_img_path, wait_time)
        '''
        
        for dl_link, img_path in zip(dl_links, img_paths):
            self.download_single_image(dl_link, img_path, wait_time)
            #print(f"{dl_link}\n{img_path}\n")
    
class Content_Analyzer():
    def deduplicate(self, input_csv_path, output_csv_path):
        if not os.path.exists(output_csv_path):
            ''' 1. Get unrepeated data '''
            df = pd.read_csv(input_csv_path)
            #print(df.shape)
            
            '''x = df[df.duplicated()]
            print(x)
            print(type(x))
            print(len(x))'''
            #spu_sku_list = list()
            unique_prods = dict()
            for index, row in list(df.iterrows()):
                #print(row)
                spu_id = str(row['productID'])[:5]
                sku_id = str(row['product_SKU_ID'])[-3:-1]
                uni_product_id = f"{spu_id}___{sku_id}"
                if uni_product_id not in unique_prods:
                    # tier2_v2
                    '''
                    unique_prods.setdefault(uni_product_id,
                                            {"product_ID": uni_product_id,
                                             "product_SPU_ID": spu_id,
                                             "product_SKU_ID": sku_id,
                                             "product_name": row["product_name"],
                                             "product_price": row["product_price"],
                                             "product_link": row["product_link"],
                                             "child_category": row["child_category"],
                                             "sales_categoryID": row["sales_categoryID"]})
                    '''
                    
                    # tier2_v3
                    unique_prods.setdefault(uni_product_id,
                                            {"product_ID": uni_product_id,
                                             "product_SPU_ID": spu_id,
                                             "product_SKU_ID": sku_id,
                                             "product_name": row["product_name"],
                                             "product_price": row["product_price"],
                                             "product_link": row["product_link"],
                                             "child_category": row["child_category"],
                                             "sales_categoryID": row["sales_categoryID"]})
                    
                else:
                    curr_child_category = row['child_category']
                    if not any([curr_child_category == existing_child_cat
                                for existing_child_cat
                                in unique_prods[uni_product_id]["child_category"].split("___")
                                if curr_child_category == existing_child_cat]):
                        unique_prods[uni_product_id]["child_category"] += f"___{curr_child_category}"
                        
                #spu_sku_list.append(f"{row['productID']}___{row['product_SKU_ID']}")
                #print(f"{row['productID']}___{row['product_SKU_ID']}")
            #print(len(unique_prods))
            #print(unique_prods["52010011___52010021"])
            #print(len(spu_sku_list))
            #print(len(set(spu_sku_list)))
            #spu_sku_list = list(set(spu_sku_list))
            #print(len(spu_sku_list))
            df = pd.DataFrame.from_dict(unique_prods,
                                        orient='index',
                                        columns=["product_ID","product_SPU_ID","product_SKU_ID",
                                                 "product_name","product_price","product_link",
                                                 "child_category","sales_categoryID"])
            #print(df.iloc[0])
        
            ''' 2. Save unrepeated data to the new csv file '''
            product_SPU_IDs, product_links = df["product_SPU_ID"], df["product_link"]
            if len(product_SPU_IDs)==len(product_links):
                df.to_csv(output_csv_path,
                          index=False,
                          encoding="utf-8-sig")
                print(f"[INFO] Writing csv file: {output_csv_path}")
            else:
                print("[WARNING] The number of `product_SPU_ID` does not equal the number of `product_link`")
    
    def modify_tier_1(self, tier_1_csv_path, output_tier_1_csv_path):
        df = pd.read_csv(tier_1_csv_path)
        categories = df["category"]
        uniform_categories = [query_uniform_category(category) for category in categories]
        df["uniform_category"] = pd.Series(uniform_categories, index=df.index)
        df.to_csv(output_tier_1_csv_path,
                  index=False,
                  encoding="utf-8-sig")
        
    def reordering_csv_records(self, tier_2_csv_path, output_csv_path):
        df = pd.read_csv(tier_2_csv_path)
    
        ''' 1. 找出所有 `SPU_ID`，並由小到大排列 '''
        # =============================================================================
        # 就算無set()也可以，因為 `tier_2_v3.csv` 已整理過，不會重複
        # =============================================================================
        product_IDs = df["product_ID"]
        SPU_IDs = df["product_SPU_ID"]
         
        SPU_IDs = sorted(list(set(SPU_IDs)))
        # P.S. The info above imply it needs 1,235 queries for pages
        #      in order to obtain all "color names" for SKU products.
        #print(SPU_IDs)
        
        ''' 2. 由排序過的 `SPU_ID` 建構 a list of dicts '''
        ordered_info = list()
        product_ID_indices = pd.Index(product_IDs)
        serial_num = 1
        for SPU_ID in SPU_IDs:
            SPU_ID = str(SPU_ID)
            
            ''' Find records with prefix: `SPU_ID` in field `product_ID` '''
            #print(product_IDs.str.contains(SPU_ID, regex=False))
            #print(f"SPU_ID: {SPU_ID}")
            prod_IDs_group = sorted(list(product_IDs[product_IDs.str.contains(SPU_ID, regex=False)]))
            
            ''' 建構 `serial_ID`(流水號 / 4位數) 
                => Serial numbers are set up for SKU products,
                   start from "0001", end with "3296"
            '''
            prod_ID___serial_ID = dict()
            for prod_ID in prod_IDs_group:
                serial_ID = str(serial_num).zfill(4)
                df_idx = product_ID_indices.get_loc(prod_ID)
                prod_ID___serial_ID[prod_ID] = (serial_ID, df_idx)
                serial_num += 1
            ordered_info.append({SPU_ID: prod_ID___serial_ID})
            #print(ordered_info)
        # =============================================================================
        # example: print out `ordered_info`
        # =============================================================================
        #fields = ["product_ID", "serial_ID", "df_idx", "product_name"]
        #fields = ["product_ID", "product_SPU_ID", "product_SKU_ID", "df_idx", "serial_ID"]
        #fields = ["product_ID", "product_SPU_ID", "product_SKU_ID", "serial_ID"]
        new_field = "serial_ID"
        fields = ["product_ID", "product_SPU_ID", "product_SKU_ID", "product_name", "product_price", "product_link", "child_category", "sales_categoryID"]
        
        ''' 3.將賦予 流水號(serial_ID) 的資料存為新的 csv file '''
        output_dict = dict()
        #output_dict.setdefault(fields[0], list())
        #output_dict.setdefault(fields[1], list())
        #output_dict.setdefault(fields[2], list())
        output_dict.setdefault(new_field, list())
        [output_dict.setdefault(field, list()) for field in fields]
        for info in ordered_info:
            for SPU_ID, tmp in info.items():
                for prod_ID, serial_ID___df_idx in tmp.items():
                    print(serial_ID___df_idx)
                    output_dict[new_field].append(serial_ID___df_idx[0])
                    [output_dict[field].append(str(df[field][serial_ID___df_idx[1]])) for field in fields]
        #print(output_dict)
        
        df_out = pd.DataFrame.from_dict(output_dict)
        #print(df_out)
        #print(len(ordered_info))
        df_out.to_csv(output_csv_path,
                      index=False,
                      encoding="utf-8-sig")

    def add_new_col_to_dataframe(self, df, list_obj, col_name):
        df[col_name] = pd.Series(list_obj, index=df.index)
        return df
    
    def get_new_column_with_info_dict(self, df, info_dict):
        new_column = list()
        current_SPU_ID = None
        for i, r in df.iterrows():
            SPU_ID = r["product_SPU_ID"]
            
            if SPU_ID not in info_dict.keys(): 
                new_column.append("X")
            else:
                if current_SPU_ID != SPU_ID:
                    current_SPU_ID = SPU_ID
                    idx = 0
                new_column.append(info_dict[current_SPU_ID][idx][1])
                idx += 1
        return new_column
    
    def extend_tier2(self, tier_2_csv_path, tier_3_csv_path, tier_4_csv_path, output_csv_path):
        ''' 1. 取得圖片路徑放入 `img_path_info` '''
        img_path_info = dict()
        df3 = pd.read_csv(tier_3_csv_path)
        #color_names = df3["SKU_color_name"]
        for i, r in df3.iterrows():
            SPU_ID, SKU_ID  = r["product_ID"].split("___")
            SPU_ID, SKU_ID = int(SPU_ID), int(SKU_ID)
            img_path_info.setdefault(SPU_ID, list())
            same_spu_list = img_path_info[SPU_ID]
            idx = 0
            if len(same_spu_list) > 0:
                for i, e in enumerate(same_spu_list):
                    if int(e[0]) > SKU_ID:  break
                    else:  idx += 1
            full_img_path = os.path.abspath(r["img_path"])
            img_path_info[SPU_ID].insert(idx, (str(SKU_ID).zfill(2), full_img_path))
            #img_path_info[SPU_ID].append((SKU_ID, r["img_path"]))
        #for k, v in img_path_info.items():
            #print(k, v, "\n")
    
        ''' 2. 取得顏色資訊放入 `color_info` '''
        color_info = dict()
        df4 = pd.read_csv(tier_4_csv_path)
        #color_names = df4["SKU_color_name"]
        for i, r in df4.iterrows():
            SPU_ID  = r["product_SPU_ID"]
            color_info.setdefault(SPU_ID, list())
            color_info[SPU_ID].append((r["new_prod_ID"], r["SKU_color_name"]))
        #curr_spu=0
        #for k, v in color_info.items():
            #print(k, v)
            #if k < curr_spu:
            #   print(k, v)
        
        ''' 3. 將圖片路徑 & 顏色資訊逐一新增，擴展原 tier_2's dataframe '''
        df2 = pd.read_csv(tier_2_csv_path)
        new_img_path_column = self.get_new_column_with_info_dict(df2, img_path_info)
        new_color_column = self.get_new_column_with_info_dict(df2, color_info)
        #print(df2.shape[0])
        #print(new_img_path_column, len(new_img_path_column))
        #print(new_color_column, len(new_color_column))
        
        ''' 4. 存成 `tier_2_v5.csv` '''
        ca = Content_Analyzer()
        df2 = ca.add_new_col_to_dataframe(df2, new_img_path_column, "img_path")
        df2 = ca.add_new_col_to_dataframe(df2, new_color_column, "SKU_color_name")
        df2.to_csv(output_csv_path, encoding="utf-8-sig", index=False)

if __name__ == "__main__":
    cw = Clothes_crawler()
    ########################
    # 爬取銷售分頁
    ########################
    '''
    cw.set_driver()
    
    #labels_info, csv_saving_type = cw.labelPage_links_crawling()
    labels_info = cw.labelPage_links_crawling()
    print(labels_info)
    
    tier1_info_list = list()
    sales_cat_idx = 1
    for genre, tmp in labels_info.items():
        for category, tmp2 in tmp.items():
            for sales_cat, link in tmp2.items():
                tier1_info = {"genre": genre,
                              "category": category,
                              "sales-category": sales_cat,
                              "link": link,
                              "sales-category ID": sales_cat_idx}
                tier1_info_list.append(tier1_info)
                sales_cat_idx += 1
    csv_path = "./res3/tier_1.csv"
    col_names = ["genre","category","sales-category","link","sales-category ID"]
    cw.save_to_csv(tier1_info_list, csv_path, col_names)
    '''
    
    ########################
    # 爬取商品詳情頁
    ########################
    '''
    page_start = 1 #149
    page_end = 174 #190
    #stop_time = 3
    for n in range(page_start, page_end+1):
    #for n in range(page_start, page_start+1):
        cw.detailPage_links_crawling(n) # 爬取第n頁
        #print(f"暫停 {stop_time} 秒...")
        #time.sleep(stop_time)
    '''
    
    ########################
    # 去除 tier2.csv 冗餘資料
    ########################
    '''
    ca = Content_Analyzer()
    input_csv_path = "./res3/tier_2.csv"
    output_csv_path = "./res3/tier_2_v3.csv"
    ca.deduplicate(input_csv_path, output_csv_path)
    #ca.deduplicate("./res3/test2.csv", output_csv_path)
    '''
    
    ########################
    # tier_1.csv => tier_1_v2.csv
    ########################
    '''
    tier_1_csv_path = "./res3/tier_1.csv"
    output_tier_1_csv_path = "./res3/tier_1_v2.csv"
    Content_Analyzer().modify_tier_1(tier_1_csv_path, output_tier_1_csv_path)
    '''
    
    ########################
    # 爬取商品圖片 (【deprecated】, last modified in 2020/12/4)
    ########################
    '''
    #cw.imgID_padding() # 增加一個欄位: img_id，便於爬圖片時逐一對應
    #cw.save_duplicated_SKUID_as_json()
    
    break_point = 100 # 爬到第幾張圖片結束 (編號從1開始),不填則預設:全部爬
    avg_wait_time = 20
    break_point, avg_wait_time = read_scrapy_setting() # 每輪等待時間,不填則預設:10秒
    # cw.download_multiple_prod_imgs()
    cw.download_multiple_prod_imgs(break_point, avg_wait_time)
    '''
    
    ########################
    # 爬取商品圖片  
    # 【suggested】, 共 3296(缺乏顏色資訊) => 3368(補爬顏色) 張
    ########################
    '''
    tier_1_csv_path = "./res3/tier_1_v2.csv"
    tier_2_csv_path = "./res3/tier_2_v3.csv"
    output_dir = "./res3/media/products/"
    tier_3_csv_path = "./res3/tier_3.csv"
    cw.crawl_images(tier_1_csv_path, tier_2_csv_path, output_dir, tier_3_csv_path)
    '''
    
    ########################
    # 清理 tier_2_v3.csv
    # （整理後放進 tier_2_v4.csv）
    '''
    P.S. 因為有些記錄在 csv 的放置順序不規律。
         即: product_ID 雖同組(same SPU_ID)資料彼此相鄰，
         但少數一些記錄的 SKU_IDs 並沒有由小到大排列。
         此程式區塊負責重新排序所有記錄。
    ref: ./res3/res3_docs/tier_2_v3_csv_資料順序不規律.PNG
    '''
    ########################
    '''
    tier_2_csv_path = "./res3/tier_2_v3.csv"
    output_csv_path = "./res3/tier_2_v4.csv"
    Content_Analyzer().reordering_csv_records(tier_2_csv_path, output_csv_path)
    '''
    
    ########################
    # 補爬 SKU 商品顏色
    # （補爬到的 SKU顏色資訊 放進 tier_4.csv）
    ########################
    '''
    tier_2_csv_path = "./res3/tier_2_v4.csv"
    output_csv_path = "./res3/tier_4.csv"
    cw.product_scrapying(tier_2_csv_path, output_csv_path)
    '''
    
    ########################
    # 剖析 tier_4.csv 的 `SKU_color_name` 欄位 (SKU商品 的 顏色名稱)
    # 將中文顏色名稱，轉為固定幾種 數值編號 儲存，
    # 並新增一個 color_mapping.csv 存放中文顏色名稱與數值編號的對應關係
    ########################
    tier_4_csv_path = "./res3/tier_4.csv"
    df = pd.read_csv(tier_4_csv_path)
    
    colors = list(df["SKU_color_name"])
    print(len(colors))
    
    color_types = list(set(df["SKU_color_name"]))
    print(len(color_types))
    
    ########################
    # 將 tier_3.csv 與 `SKU商品` 對應的 `img_path`(本機圖片路徑)
    # (X)和 tier_4.csv 與 `SKU商品` 對應的 `SKU_color_name`(顏色名稱)
    # 加入(主要的商品資料表) tier_2_v4.csv 中，存為 tier_2_v5.csv
    ########################
    '''
    tier_3_csv_path = "./res3/tier_3.csv"
    tier_4_csv_path = "./res3/tier_4.csv"
    tier_2_csv_path = "./res3/tier_2_v4.csv"
    output_csv_path = "./res3/tier_2_v5.csv"
    Content_Analyzer().extend_tier2(tier_2_csv_path, tier_3_csv_path, tier_4_csv_path, output_csv_path)
    '''
    
    ########################
    # 手動複製一份 tier_2_v5.csv 為 tier_2_v6.csv
    # 並逐一補齊缺失顏色名稱，此 block 可驗證是否都補齊
    ########################
    '''
    tier_2_csv_path = "./res3/tier_2_v6.csv"
    df = pd.read_csv(tier_2_csv_path)
    color_names = df["SKU_color_name"]
    color_counts = color_names.value_counts()["X"]
    print(color_counts) # 32
    #color_counts = len(df[df["SKU_color_name"]=="X"])
    #print(color_counts) # 32
    '''
    
    
