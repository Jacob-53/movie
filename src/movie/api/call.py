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
    
    
def list2df(data:list, date: str,url_params={}):
    df= pd.DataFrame(data)
    df["dt"]=date
    #df["multiMovieYn"]="Y"
    for k,v in url_params.items():
        df[k] = v
        
    num_col=["rnum","rank","rankInten","movieCd","salesAmt","salesShare","salesInten","salesChange","salesAcc","audiCnt","audiInten","audiChange","audiAcc","scrnCnt","showCnt"]
    df[num_col]=df[num_col].apply(pd.to_numeric)
       
    return df

def save_df(df: pd.DataFrame, base_path : str,partitions=['dt'],url_params={}):
    df.to_parquet(base_path,partition_cols=partitions)
    save_path = f"{base_path}"
    partition=list(url_params.keys())
    for i in partitions:
        save_path= save_path + f"/{i}={df[i][0]}"
    for k in partition:
        save_path=save_path+f"/{k}"
    return save_path
  # if not os.path.exists(path): 
    #     os.makedirs(path)
    #     print(f"폴더 생성됨: {path}")
    
    # else:
    #     pass
    # sdf=data.to_csv(f"{path}/df.csv",index=False)
    
    # return sdf

def list2df_check_num():
    a= call_api("20250316",url_param={})
    dfa=pd.DataFrame(a)
    num_col=["rnum","rank","rankInten","movieCd","salesAmt","salesShare","salesInten","salesChange","salesAcc","audiCnt","audiInten","audiChange","audiAcc","scrnCnt","showCnt"]
    dfs=dfa[num_col].apply(pd.to_numeric)
    return dfs
