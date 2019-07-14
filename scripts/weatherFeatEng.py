import pandas as pd
import pytz
import glob
from pytz import timezone
from tqdm import tqdm
from tqdm import tqdm
import numpy as np
DIR=r"D:\experiments\dataEngineering\input_data\\"


weather_file_list=glob.glob(DIR+"weather\*.parquet")
drive_file_list=glob.glob(DIR+"drive\*.parquet")
ordered_cols=['vehicle_id',
'week_start_date',
'total_light_rain_driving_km',
'total_light_freezing_rain_driving_km',
'total_light_snow_driving_km',
'total_moderate_rain_driving_km',
'total_moderate_freezing_rain_driving_km',
'total_moderate_snow_driving_km',
'total_heavy_rain_driving_km'
]

def read_files(drive_file_list):
    return pd.concat([pd.read_parquet(f) for f in drive_file_list])

def k_to_f(k):
    return (((k-273)*9)/5)+32

def unit_converstion(df):
    df["temp_f"]=df['temperature_data'].apply(k_to_f)
    return df

def timezone_conversion(df,trip):
    trip['datetime']=trip['datetime'].dt.tz_localize("US/Pacific")
    df['datetime']=pd.to_datetime(df['date'].astype("str")+" "+df['time'])
    df['datetime']=df['datetime'].dt.tz_convert("US/Pacific")
    return df,trip

def haversine_np(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)

    All args must be of equal length.    

    """
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2

    c = 2 * np.arcsin(np.sqrt(a))
    km = 6367 * c
    return km

#print (df)


def merge(df,trip):
    trip=trip.rename(columns={'long':'lon'})
    merged1 = pd.merge(df,trip,on='datetime')
    merged1['dist'] = haversine_np(merged1['lon_x'],merged1['lat_x'],merged1['lon_y'],merged1['lat_y'])
    return merged1




def feat_creation(merged12):
    merged12["snow"]=merged12['temp_f']<=27
    merged12["freeze_rain"]=(merged12["temp_f"]>27)&(merged12["temp_f"]<=32)
    merged12["rain"]=merged12["temp_f"]>32
    merged12["light"]=(merged12["precipitation_data"]>0)&(merged12["precipitation_data"]<=2.5)
    merged12["moderate"]=(merged12["precipitation_data"]>2.5)&(merged12["precipitation_data"]<=7.6)
    merged12["heavy"]=(merged12["precipitation_data"]>7.6)
    return merged12


# def vehicle_wise_weekly_agg(df,agg_cols):
#     grouped=df.groupby('vehicle_id')
#     res_df=[]
#     for vehicle_id,df in tqdm(grouped,desc="combining-result"):
        
#         temp=df.set_index(pd.DatetimeIndex(df['datetime']))
#         temp=temp.resample("W-Mon")[agg_cols].sum()
#         temp['vehicle_id']=vehicle_id
#         temp['week_start_date']=temp.index.date
#         temp['total_light_rain_driving_km']=temp[temp['rain']&temp['light']].resample("W-Mon")['dist'].sum().values
#         temp["total_light_freezing_rain_driving_km"]=temp[temp['freeze_rain']&temp['light']].resample("W-Mon")['dist'].sum().values
#         temp["total_light_snow_driving_km"]=temp[temp['snow']&temp["light"]].resample("W-Mon")['dist'].sum().values
#         temp["total_moderate_rain_driving_km"]=temp[temp['rain']&temp["moderate"]].resample("W-Mon")['dist'].sum().values
#         temp["total_moderate_freezing_rain_driving_km"]=temp[temp['freeze_rain']&temp["moderate"]].resample("W-Mon")['dist'].sum().values
#         temp["total_moderate_snow_driving_km"]=temp[temp['snow']&temp['moderate']].resample("W-Mon")['dist'].sum().values
#         temp["total_heavy_rain_driving_km"]=temp[temp['rain']&temp["heavy"]].resample("W-Mon")['dist'].sum().values
#         #temp=temp[temp['rain']].resample("W-Mon")['dist'].sum().values

#         res_df.append(temp)

#     return pd.concat(res_df).sort_values(by=['vehicle_id',"week_start_date"])[ordered_cols]

#from tqdm import tqdm
def vehicle_wise_weekly_agg(df,agg_cols):
    grouped=df.groupby('vehicle_id')
    res_df=[]
    for vehicle_id,df in tqdm(grouped,desc="combining-result"):
        
        temp=df.set_index(pd.DatetimeIndex(df['datetime']))
        #print(temp.head())
        temp['week_start_date']=temp.index.date
        temp1=temp.resample("W-Mon").count()
        temp1['vehicle_id']=vehicle_id
        print(temp1.head())
        temp1['total_light_rain_driving_km']=temp[temp['rain']&temp['light']].resample("W-Mon")['dist'].sum()
        temp1["total_light_freezing_rain_driving_km"]=temp[temp['freeze_rain']&temp['light']].resample("W-Mon")['dist'].sum()#.values
        temp1["total_light_snow_driving_km"]=temp[temp['snow']&temp["light"]].resample("W-Mon")['dist'].sum()#.values
        temp1["total_moderate_rain_driving_km"]=temp[temp['rain']&temp["moderate"]].resample("W-Mon")['dist'].sum()#.values
        temp1["total_moderate_freezing_rain_driving_km"]=temp[temp['freeze_rain']&temp["moderate"]].resample("W-Mon")['dist'].sum()#.values
        temp1["total_moderate_snow_driving_km"]=temp[temp['snow']&temp['moderate']].resample("W-Mon")['dist'].sum()#.values
        temp1["total_heavy_rain_driving_km"]=temp[temp['rain']&temp["heavy"]].resample("W-Mon")['dist'].sum()#.values
        #temp=temp[temp['rain']].resample("W-Mon")['dist'].sum().values
        #temp1['week_start_date']=temp.index.date

        res_df.append(temp1)

    return pd.concat(res_df).sort_values(by=['vehicle_id',"week_start_date"])[ordered_cols]



if __name__=="__main__":
    df=read_files(weather_file_list)
    trip=read_files(drive_file_list)
    df=unit_converstion(df)
    df,trip=timezone_conversion(df,trip)
    merged=merge(df,trip)
    merged=feat_creation(merged)
    res=vehicle_wise_weekly_agg(merged,['dist'])

    res.to_csv(DIR+"weather_features.csv")