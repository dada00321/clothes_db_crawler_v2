import pandas as pd
import json

class Checker():
    def check_duplicate_SKU_IDs(self):
        csv_path = ".././res/tier_2.csv"
        df = pd.read_csv(csv_path, header=0)
        #product_SKU_IDs = df["product_SKU_ID"]
        #print(df.loc[:3]) # 取 rows, [type]:DataFrame
        #print(df["product_SKU_ID"]) # 取 column, [type]: Series
        
        duplicated_SKU_IDs = list()
        product_SKU_ID_list = list(df["product_SKU_ID"])
        for SKU_ID in set(product_SKU_ID_list):
            appear_times = product_SKU_ID_list.count(SKU_ID)
            if appear_times > 1:
                #duplicated_SKU_IDs.setdefault(SKU_ID, appear_times)
                duplicated_SKU_IDs.append({"SKU_ID": SKU_ID,
                                           "appear_times": appear_times})
        #print(f"duplicated_SKU_IDs:\n{duplicated_SKU_IDs}")
        return duplicated_SKU_IDs
    
    def save_to_json(self, multi_dict_list, dest_path):
        #json_duplicated_SKU_IDsc = json.dumps(multi_dict_list)
        json_file = json.dumps(multi_dict_list)
        print(json_file) # type: str
        with open(dest_path, encoding="utf-8", mode="w") as f:
            try:
                f.write(json_file)
                print("記錄重複SKU_ID的json檔儲存成功！")
            except:
                print("記錄重複SKU_ID的json檔儲存失敗")
"""
if __name__ == "__main__":
    checker = Checker()
    path = "../res/duplicated_SKU_IDs.json"
    duplicated_SKU_IDs = checker.check_duplicate_SKU_IDs()
    checker.save_to_json(duplicated_SKU_IDs, path)
"""
    
    