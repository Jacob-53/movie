from movie.api.call import gen_url,call_api,list2df,save_df,list2df_check_num
import os
import requests
import pandas as pd



def test_gen_url():
    r = gen_url()
    assert "kobis" in r
    assert "targetDt" in r
    assert os.getenv("MOVIE_KEY") in r
    
def test_gen_url_default():
    r = gen_url(url_param={"multiMovieYn":"N","repNationCd":"K"})
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
    read_df = pd.read_parquet(r)
    assert 'dt' not in read_df.columns
    assert 'dt' in pd.read_parquet(base_path).columns
 
 
def test_list2df_check_num():
    #from pandas.api.types import is_numeric_dtype
    # a=list2df_check_num()
    # print(a)
    # for i in a.columns:
    #     assert is_numeric_dtype(a[i])
    # 위 아래 2가지 방법 있음
    a= list2df_check_num()
    num_col=["rnum","rank","rankInten","movieCd","salesAmt","salesShare","salesInten","salesChange","salesAcc","audiCnt","audiInten","audiChange","audiAcc","scrnCnt","showCnt"]
    for i in num_col:
        assert a[i].dtype in ['int','float64'], f"{i}가  숫자가 아님"