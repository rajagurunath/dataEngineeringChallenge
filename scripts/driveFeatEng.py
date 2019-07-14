import pandas as pd 
import numpy as np 
import glob 

DIR="D:\experiments\dataEngineering\input_data\\"
trip_file_list=glob.glob(DIR+"trip\*.parquet")

def read_files(file_list):
    return pd.concat([pd.read_parquet(f) for f in file_list])

def cal_acceleration(df):
    fn=lambda trip1:trip1['velocity'].diff()/trip1['datetime'].dt.second.diff()
    res=[]
    for name,df1 in df.groupby(df.trip_id):
        print(name)
        acc=fn(df1).values
        df1["acceleration"]=acc
        df1["trip_id"]=name        
        df1["time_taken"]=df1.datetime.dt.second
        res.append(df1)
    return pd.concat(res)

add_seconds=lambda df:df.datetime.dt.second
def cal_peaks(trip):
    trip['acceleration_minus_one_shift']=trip['acceleration'].shift(-1)


# def cnt_decceleration(acceleration,shl_fn,lng_fn):
#     shl=[]
#     lng=[]
#     ft_10=0
#     tl=np.sign(acceleration)
#     prev=tl[0]
#     for idx,curr in enumerate(tl[1:],start=1):
#         if curr<0:
#             if curr==prev:
#                 shl.append(acceleration[idx-1])
#                 shl.append(acceleration[idx])
#                 print(shl)
#             else:
#                 if len(shl)>0:
#                     ft_10+=shl_fn(shl)  
#                 lng.append(acceleration[idx])
#                 shl=[]
#         print(curr,prev)
#         prev=curr
#     ft_10+=lng_fn(lng)
#     if len(shl)>0:
#         ft_10+=shl_fn(shl)
#     print(ft_10)
#     return ft_10
# def cnt_acceleration(acceleration,shl_fn,lng_fn):
#     shl=[]
#     lng=[]
#     ft_10=0
#     tl=np.sign(acceleration)
#     prev=tl[0]
#     for idx,curr in enumerate(tl[1:],start=1):
#         if curr>0:
#             if curr==prev:
#                 shl.append(acceleration[idx-1])
#                 shl.append(acceleration[idx])
#                 print(shl)
#             else:
#                 if len(shl)>0:
#                     ft_10+=shl_fn(shl)  
#                 lng.append(acceleration[idx])
#                 shl=[]
#         print(curr,prev)
#         prev=curr
#     ft_10+=lng_fn(shl)
#     if len(shl)>0:
#         ft_10+=shl_fn(shl)
#     print(ft_10)
#     return ft_10
# def cnt_acceleration(acceleration,shl_fn,lng_fn):
#     shl=[]
#     lng=[]
#     ft_10=0
#     tl=np.sign(acceleration)
#     prev=tl[0]
#     for idx,curr in enumerate(tl[1:],start=1):
#         if curr>0:
#             if curr==prev:
#                 shl.append(acceleration[idx-1])
#                 shl.append(acceleration[idx])
#                 print(shl)
#             else:
#                 if len(shl)>0:
#                     ft_10+=shl_fn(shl)  
#                     print("shl",ft_10)
#                     shl=[]
#                 lng.append(acceleration[idx])
#         print(curr,prev)
#         prev=curr
#     print('lng',lng)
#     ft_10+=lng_fn(lng)
#     if len(shl)>0:
#         print("shl",ft_10)
#         ft_10+=shl_fn(shl)
#         print(ft_10)
#     print(ft_10)
#     return ft_10

# continous_decel_val=lambda x:min(x)<=-10
# def continous_decel_list(list_,value=-10):
#     print("less 10",list_)
#     cnt=0
#     for i in list_:
#         if i<=value:
#             cnt+=1
#     return cnt

# continous_acel_val=lambda x:min(x)>=10
# def continous_acel_list(list_,value=10):
#     print("less 10",list_)
#     cnt=0
#     for i in list_:
#         if i>=value:
#             cnt+=1
#     return cnt

# continous_decel_val=lambda x:min(x)<=-10
# def continous_decel_list(list_,value=-10):
#     print("less 10",list_)
#     cnt=0
#     for i in list_:
#         if i<=value:
#             cnt+=1
#     return cnt

continous_acel_val=lambda x:min(x)>=10
def continous_acel_list(list_,value=10):
     #print("less 10",list_)
     cnt=0
     for i in list_:
         if i>=value:
             cnt+=1
     return cnt

def cnt_decceleration(acceleration,shl_fn,lng_fn):
    acceleration=acceleration.tolist()
    shl=[]
    lng=[]
    ft_10=0
    tl=np.sign(acceleration)
    prev=tl[0]
    for idx,curr in enumerate(tl[1:],start=1):
        if curr<0:
            if curr==prev:
                shl.append(acceleration[idx-1])
                shl.append(acceleration[idx])
            else:
                if len(shl)>0:
                    ft_10+=shl_fn(shl)  
                lng.append(acceleration[idx])
                shl=[]
        prev=curr
    ft_10+=lng_fn(lng)
    if len(shl)>0:
        ft_10+=shl_fn(shl)
    print("dec",ft_10)
    return ft_10
def cnt_acceleration(acceleration,shl_fn,lng_fn):
    acceleration=acceleration.tolist()
    shl=[]
    lng=[]
    ft_10=0
    tl=np.sign(acceleration)
    prev=tl[0]
    for idx,curr in enumerate(tl[1:],start=1):
        if curr>0:
            if curr==prev:
                shl.append(acceleration[idx-1])
                shl.append(acceleration[idx])
            else:
                if len(shl)>0:
                    ft_10+=shl_fn(shl)  
                    shl=[]
                lng.append(acceleration[idx])
        prev=curr
    ft_10+=lng_fn(lng)
    if len(shl)>0:
        ft_10+=shl_fn(shl)
    print("acc",ft_10)
    return ft_10

continous_decel_val=lambda x:(min(x)<=-10)*1
def continous_decel_list(list_,value=-10):
    cnt=0
    for i in list_:
        if i<=value:
            cnt+=1
    return cnt

continous_decel_val2=lambda x:((min(x)>-10)&(min(x)<=-3))*1
def continous_decel_list2(list_,value1=-10,value2=-3):
    cnt=0
    for i in list_:
        if (i>value1)&(i<=value2):
            cnt+=1
    return cnt

continous_acel_val2=lambda x:((max(x)<10)&(max(x)>=3))*1
def continous_acel_list2(list_,value1=3,value2=10):
    cnt=0
    for i in list_:
        if (i<value2)&(i>=value1):
            cnt+=1
    return cnt

# def acctime(df):
#     df['acc_sign']=np.sign(df['acceleration'])
#     df=df[df['acc_sign']>0]
#     r=df.groupby('acc_sign')['time_taken'].sum()
#     return r.values[0]
# def acctime(df):
#     df['acc_sign']=np.sign(df['acceleration'])
#     r=df[df['acc_sign']>0]['datetime'].max()-df[df['acc_sign']>0]['datetime'].min()
#     return r
def acctime(df):
    df['acc_sign']=np.sign(df['acceleration'])
    r=(df[df['acc_sign']>0]['datetime'].max()-df[df['acc_sign']>0]['datetime'].min()).seconds
    return r

def deacctime(df):
    # df['acc_sign']=np.sign(df['acceleration'])
    # df=df[df['acc_sign']<0]
    # r=df.groupby('acc_sign')['time_taken'].sum()
    # return r.values[0]# ft_10,less_than_minus10(lng)
    df['acc_sign']=np.sign(df['acceleration'])
    r=(df[df['acc_sign']<0]['datetime'].max()-df[df['acc_sign']<0]['datetime'].min()).seconds
    return r

def return_count_deaccel(acceleration):
    accsign=np.sign(acceleration)
    r=acceleration.loc[(accsign==-1)&(accsign.shift()!=accsign)]
    return r.shape[0]
def return_count_accel(acceleration):
    accsign=np.sign(acceleration)
    r=acceleration.loc[(accsign==1)&(accsign.shift()!=accsign)]
    return r.shape[0] 

# if len(shl)>0:
#     ft_10+=less_than_minus10(shl)
# print(ft_10)

if __name__=='__main__':

    trip_df=read_files(trip_file_list)
    trip_df=cal_acceleration(trip_df)
    trip_df['seconds']=add_seconds(trip_df)
    grouped=trip_df.groupby("trip_id")
    columns=['trip_id','ft_cnt_vehicle_deaccel_val','ft_sum_time_accel_val','ft_sum_time_deaccel_val',
    	'ft_cnt_vehicle_accel_val','ft_sum_hard_brakes_10_flg_val',
        	'ft_sum_hard_brakes_3_flg_val','ft_sum_hard_accel_10_flg_val',
            	'ft_sum_hard_accel_3_flg_val']
    res_df=pd.DataFrame()
    res_df['ft_cnt_vehicle_deaccel_val']=grouped['acceleration'].apply(return_count_deaccel)
    res_df['ft_sum_hard_brakes_10_flg_val']=grouped['acceleration'].apply(cnt_decceleration,\
        continous_decel_val,continous_decel_list)
    res_df['ft_sum_hard_brakes_3_flg_val']=grouped['acceleration'].apply(cnt_decceleration,\
        continous_decel_val2,continous_decel_list2)
    res_df['ft_sum_time_deaccel_val']=grouped.apply(deacctime)

    
    res_df['ft_cnt_vehicle_accel_val']=grouped['acceleration'].apply(return_count_accel)
    res_df['ft_sum_hard_accel_10_flg_val']=grouped['acceleration'].apply(cnt_acceleration,\
        continous_acel_val,continous_acel_list)
    res_df['ft_sum_hard_accel_3_flg_val']=grouped['acceleration'].apply(cnt_acceleration,\
        continous_acel_val2,continous_acel_list2)
    res_df['ft_sum_time_accel_val']=grouped.apply(acctime)
    #res_df["trip_id"]=list(grouped.groups.keys())
    res_df=res_df.sort_index()
    #res_df.sort_values(by=res_df.index)
    res_df[columns[1:]].to_csv(DIR+"drive_features.csv")
    


