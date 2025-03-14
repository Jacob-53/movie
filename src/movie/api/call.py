import os
import requests
import pandas as pd



BASE_URL = "http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
KEY=os.getenv("MOVIE_KEY")

def gen_url(dt="20120101",url_param={}):
    #key=",".join(url_param.keys())
    #value=",".join(url_param.values())
    url = f"{BASE_URL}?key={KEY}&targetDt={dt}"
    for k,v in url_param.items():
        url = url + f"&{k}={v}"
    return url

def call_api(dt="20120101",url_param={}):
    try:
        res = requests.get(gen_url(dt,url_param))
        data = res.json()
        if res.status_code == 200:
            a = data["boxOfficeResult"]["dailyBoxOfficeList"]
            return a
    except requests.exceptions.RequestException as e:
        return f"API 요청실패{e}"
    
    
def list2df(data:list, date: str):
    df= pd.DataFrame(data)
    df["dt"]=date   
    return df

def save_df(df: pd.DataFrame, base_path : str ):
    # if not os.path.exists(path): 
    #     os.makedirs(path)
    #     print(f"폴더 생성됨: {path}")
    
    # else:
    #     pass
    # sdf=data.to_csv(f"{path}/df.csv",index=False)
    
    # return sdf
    df.to_parquet(base_path,partition_cols=['dt'])
    save_path = f"{base_path}/dt={df['dt'][0]}"
    return save_path