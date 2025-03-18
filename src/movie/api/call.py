import os
import requests
import pandas as pd


BASE_URL = "http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
KEY = os.getenv("MOVIE_KEY")

def gen_url(dt="20120101",url_param={}):
    #key=",".join(url_param.keys())
    #value=",".join(url_param.values())
    url = f"{BASE_URL}?key={KEY}&targetDt={dt}"
    for k,v in url_param.items():
        url = url + f"&{k}={v}"
    return url

def call_api(dt="20120101",url_param={}):
    try:
        res = requests.get(gen_url(dt, url_param))
        data = res.json()
        if res.status_code == 200:
            a = data["boxOfficeResult"]["dailyBoxOfficeList"]
            return a
    except requests.exceptions.RequestException as e:
        return f"API 요청실패{e}"
    
    
def list2df(data:list, date: str,url_param={}):
    df= pd.DataFrame(data)
    df["dt"]=date
    #df["multiMovieYn"]="Y"
    for k,v in url_param.items():
        df[k] = v
        
    num_col=['rnum', 'rank', 'rankInten', 'salesAmt', 'audiCnt',
                'audiAcc', 'scrnCnt', 'showCnt', 'salesShare', 'salesInten',
                'salesChange', 'audiInten', 'audiChange']
    df[num_col]=df[num_col].apply(pd.to_numeric)
       
    return df

# def save_df(df, base_path,partitions):
#     df.to_parquet(base_path, partition_cols=['dt'])
#     save_path = f"{base_path}/dt={df['dt'][0]}"
#     return save_path



def save_df(df: pd.DataFrame, base_path : str, partitions=['dt']):
    
    df.to_parquet(base_path, partition_cols=partitions)
    
    save_path = f"{base_path}"
    for i in partitions:
        save_path= save_path + f"/{i}={df[i][0]}"
        
    return save_path

def merge_df(ds_nodash,base_path):
    df=pd.read_parquet(base_path/f"dt={ds_nodash}")
    df=df.drop(columns=['rnum', 'rank', 'rankInten', 'salesShare'])
    fil_movieCd=[]
    for _, row in df.iterrows():
        if pd.isna(row['multiMovieYn']) or pd.isna(row['repNationCd']):
            fil_movieCd.append(row['movieCd'])
    
    def merge_values(series):
        return ', '.join(series.dropna().astype(str).unique())

    merged_list=[]    

    for i in set(fil_movieCd):
        fil_dup=df[df['movieCd'] == i][['movieCd', 'movieNm', 'multiMovieYn', 'repNationCd']]
        if len(fil_dup) == 1 and fil_dup[['multiMovieYn', 'repNationCd']].isna().all(axis=1).iloc[0]:
            fil_dup = fil_dup.fillna("Unclassified")
            merged_list.append(fil_dup)
        else:
            fil_dup=fil_dup.dropna(subset=['multiMovieYn', 'repNationCd'], how='all')
            merged_adf = fil_dup.groupby(['movieCd', 'movieNm'], as_index=False).agg({
                        'multiMovieYn': merge_values,
                        'repNationCd': merge_values
                        })
        merged_list.append(merged_adf)
    f_merged_df = pd.concat(merged_list, ignore_index=True)
    f_merged_df






  # if not os.path.exists(path): 
    #     os.makedirs(path)
    #     print(f"폴더 생성됨: {path}")
    
    # else:
    #     pass
    # sdf=data.to_csv(f"{path}/df.csv",index=False)
    
    # return sdf

# def list2df_check_num():
#     a= call_api("20250316",url_param={})
#     dfa=pd.DataFrame(a)
#     num_col=["rnum","rank","rankInten","movieCd","salesAmt","salesShare","salesInten","salesChange","salesAcc","audiCnt","audiInten","audiChange","audiAcc","scrnCnt","showCnt"]
#     dfs=dfa[num_col].apply(pd.to_numeric)
#     return dfs
