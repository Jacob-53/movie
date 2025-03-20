from movie.api.call import gen_url, call_api, list2df, save_df
import os
import pandas as pd

def test_gen_url():
    r = gen_url()
    assert "kobis" in r
    assert "targetDt" in r
    assert os.getenv("MOVIE_KEY") in r
    
def test_gen_url_default():
    r = gen_url(url_param={"multiMovieYn":"N","repNationCd":"K"})
    print(r)
    assert "&multiMovieYn=N" in r
    assert "&repNationCd=K" in r
    
def test_call_api():
    r= call_api()
    assert isinstance(r,list)
    assert isinstance(r[0]['rnum'],str)
    assert len(r) == 10
    for i in r:
        assert isinstance(i,dict)
        
def test_list2df():
    ymd = "20210101"
    data = call_api(dt=ymd)
    df = list2df(data,ymd)
    assert isinstance(df, pd.DataFrame)
    assert len(data) == len(df)
    assert set(data[0].keys()).issubset(set(df.columns)) #==> 1,2,3 issubset 1,2,3,4,5 ==> True
    assert "dt" in df.columns, "dt 컬럼이 있어야 함"
    assert (df["dt"] == ymd).all(), "모든 컬럼에 입력된 날짜 값이 존재 해야 함"
    
def test_save_df():
    ymd = "20210101"
    data = call_api(dt=ymd)
    df = list2df(data, ymd)
    base_path = "~/temp/movie"
    r = save_df(df, base_path)
    assert r == f"{base_path}/dt={ymd}"
    print("save_path", r)
    read_df = pd.read_parquet(r)
    assert 'dt' not in read_df.columns
    assert 'dt' in pd.read_parquet(base_path).columns
 
 
# def test_list2df_check_num():
#     #from pandas.api.types import is_numeric_dtype
#     # a=list2df_check_num()
#     # print(a)
#     # for i in a.columns:
#     #     assert is_numeric_dtype(a[i])
#     # 위 아래 2가지 방법 있음
#     a= list2df_check_num()
#     num_col=["rnum","rank","rankInten","movieCd","salesAmt","salesShare","salesInten","salesChange","salesAcc","audiCnt","audiInten","audiChange","audiAcc","scrnCnt","showCnt"]
#     for i in num_col:
#         assert a[i].dtype in ['int','float64'], f"{i}가  숫자가 아님"
        
def test_common_get_data():
    ds_nodash="20240101"
    url_param={"multiMovieYn":"Y"}
    
    from movie.api.call import call_api,list2df,save_df
    import os
    
    data = call_api(ds_nodash, url_param)
    df = list2df(data, ds_nodash, url_param)
    sv = save_df(df,"~/data/movies/dailyboxoffice",partitions=['dt'] + list(url_param.keys()))
    assert os.path.expanduser("~/data/movies/dailyboxoffice/dt=20240101")
    
    
    
def test_save_df_url_parmas():
    from movie.api.call import call_api,list2df,save_df
    ymd = "20210101"
    url_param={"multiMovieYn":"Y"}
    base_path = "~/data/movie"
    
    data = call_api(dt=ymd,url_param=url_param)
    df = list2df(data, ymd,url_param)
    
    # partition=[]
    # for v in url_param.values():
    #     for item in v.items():
    #         partition.append(item)
    # for k, v in partition:
    #     save_path=save_path+f"/{k}={v}"
    #r = save_df(df, base_path,['dt','multiMovieYn'])
    #r = save_df(df, base_path,['dt'] + list(url_param.keys()))
    
    partitions = ['dt'] + list(url_param.keys())
    r = save_df(df, base_path, partitions=partitions)
    assert r == f"{base_path}/dt={ymd}/multiMovieYn=Y"
    read_df = pd.read_parquet(r)
    assert 'dt' not in read_df.columns
    assert 'dt' in pd.read_parquet(base_path).columns
    
def test_merge_df():
    base_path="/home/jacob/data/movies/dailyboxoffice"
    ds_nodash="20240101"
    svbase_path =  "/home/jacob/data/movies/merge/dailyboxoffice"
    save_path = f"{svbase_path}/dt={ds_nodash}/merged.parquet"
    df=pd.read_parquet(f"{base_path}/dt={ds_nodash}")
    df.drop(columns=['rank', 'rnum', 'rankInten', 'salesShare'])
    assert len(df) == 50
    
    fil_movieCd=[]
    for _, row in df.iterrows():
        if pd.isna(row['multiMovieYn']) or pd.isna(row['repNationCd']):
            fil_movieCd.append(row['movieCd'])
    
    def merge_values(series):
        return ', '.join(series.dropna().astype(str).unique())

    fil_movieCd=[]
    
    for _, row in df.iterrows():
        if pd.isna(row['multiMovieYn']) or pd.isna(row['repNationCd']):
            fil_movieCd.append(row['movieCd'])
    
    def merge_values(series):
        return ', '.join(series.dropna().astype(str).unique())

    merged_list=[]    

    for i in set(fil_movieCd):
        fil_dup=df[df['movieCd'] == i][['movieCd', 'movieNm', 'multiMovieYn', 'repNationCd','audiCnt','rnum']]
        if len(fil_dup) == 1 and fil_dup[['multiMovieYn', 'repNationCd']].isna().all(axis=1).iloc[0]:
            fil_dup = fil_dup.fillna("Unclassified")
            merged_list.append(fil_dup)
        else:
            fil_dup=fil_dup.dropna(subset=['multiMovieYn', 'repNationCd'], how='all')
            merged_df = fil_dup.groupby(['movieCd', 'movieNm'], as_index=False).agg({
                        'multiMovieYn': merge_values,
                        'repNationCd': merge_values,
                        'audiCnt': 'max'
                        })
        merged_list.append(merged_df)
        
    f_merged_df = pd.concat(merged_list, ignore_index=True)
    f_merged_df['rank'] =f_merged_df['audiCnt'].rank(ascending=False,method='dense')
    unique_df_sorted = f_merged_df.sort_values(by='rank')
    unique_df_sorted[['multiMovieYn', 'repNationCd']] = unique_df_sorted[['multiMovieYn', 'repNationCd']].replace('', pd.NA)
    save_dir = os.path.dirname(save_path)
    os.makedirs(save_dir, exist_ok=True)
    unique_df_sorted.to_parquet(save_path)
    assert len(unique_df_sorted) == 25
    assert unique_df_sorted['multiMovieYn'].isna().sum() == 5
    assert unique_df_sorted['repNationCd'].isna().sum() == 5
    assert not unique_df_sorted[['multiMovieYn', 'repNationCd']].isna().all(axis=1).all()
    assert unique_df_sorted.iloc[0]['movieNm'] == '노량: 죽음의 바다'
    assert os.path.exists(save_path)
    
    
def test_gen_meta():
    base_path = "/home/jacob/data/movie-after/meta"
    rbase_path = "/home/jacob/data/movies/merge/dailyboxoffice"
    save_path = f"{base_path}/meta.parquet"
    start_date = 20240101
    ds_nodash = 20240102
    
    if not os.path.exists(base_path):
        os.makedirs(base_path)
    else:
        pass
    
    if start_date == ds_nodash:
        df=pd.read_parquet(f"{rbase_path}/dt={ds_nodash}")
        df.to_parquet(save_path)
   
    # adf = pd.read_parquet(f"{rbase_path}/dt={ds_nodash}")
    # bdf = pd.read_parquet(save_path)
    
    # assert len(adf) == len(bdf)
    # assert adf.equals(bdf), "adf는 bdf 동일해야 합니다!"
    
    else:
        today_df = pd.read_parquet(f"{rbase_path}/dt={ds_nodash}")
        target_df = pd.read_parquet(save_path)
        target_df.set_index("movieCd",inplace=True)
        today_df.set_index("movieCd",inplace=True)
        f_target_df = target_df.combine_first(today_df)
        f_target_df.reset_index(inplace=True)
        f_target_df.to_parquet(save_path)
    
    bdf = pd.read_parquet(save_path)
    assert len(f_target_df) == len(bdf)
    