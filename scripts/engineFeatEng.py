import pandas as pd
import pytz
import glob
from pytz import timezone
from tqdm import tqdm
DIR=r"D:\experiments\dataEngineering\input_data\\"


drive_file_list=glob.glob(DIR+"drive\*.parquet")

def read_files(drive_file_list):
    return pd.concat([pd.read_parquet(f) for f in drive_file_list])

def time_zone_conversion(df):
    df['datetime_pst']=df['datetime'].dt.tz_localize(timezone('US/Pacific'))
    return df.set_index(pd.DatetimeIndex(df['datetime_pst']))

def merge_vechicle(df,vechicle):
    return df.merge(vechicle,on='vehicle_id',how='outer')

def add_date(df):
    df['week_start_date']=df['datetime_pst'].dt.date
    df['seconds']=(df['datetime_pst'].dt.minute/60)
    return df

def engine_feature_creation(merged_df):
    merged_df['active_horse_pow']=(merged_df['eng_load']/255)*\
        merged_df['max_torque']*merged_df['rpm']/5252
    merged_df['horse_pow_util']=merged_df['active_horse_pow']/merged_df['max_horsepower']
    merged_df['torque_util']=merged_df['eng_load']/255
    merged_df['rpm_util']=merged_df['rpm']/merged_df['max_horsepower_rpm']

    return merged_df

def feature_aggregations(engine_feat):
    engine_feat['ft_torque_util_60pct_s']=engine_feat[(0.60<=engine_feat['torque_util'])&\
        (engine_feat['torque_util']<0.70)]['seconds']
    engine_feat['ft_torque_util_70pct_s']=engine_feat[(0.70<=engine_feat['torque_util'])&\
        (engine_feat['torque_util']<0.80)]['seconds']
    engine_feat['ft_torque_util_80pct_s']=engine_feat[(0.80<=engine_feat['torque_util'])&\
        (engine_feat['torque_util']<0.90)]['seconds']
    engine_feat['ft_torque_util_90pct_s']=engine_feat[(0.90<=engine_feat['torque_util'])&\
        (engine_feat['torque_util']<1)]['seconds']

    engine_feat['ft_horsepower_util_50pct_s']=engine_feat[(0.50<=engine_feat['horse_pow_util'])&\
        (engine_feat['horse_pow_util']<0.60)]['seconds']
    engine_feat['ft_horsepower_util_60pct_s']=engine_feat[(0.60<=engine_feat['horse_pow_util'])&\
        (engine_feat['horse_pow_util']<0.70)]['seconds']
    engine_feat['ft_horsepower_util_70pct_s']=engine_feat[(0.70<=engine_feat['horse_pow_util'])&\
        (engine_feat['horse_pow_util']<0.80)]['seconds']
    engine_feat['ft_horsepower_util_80pct_s']=engine_feat[(0.80<=engine_feat['horse_pow_util'])&\
        (engine_feat['horse_pow_util']<0.90)]['seconds']

    engine_feat['ft_rpm_util_50pct_s']=engine_feat[(0.50<=engine_feat['rpm_util'])&\
        (engine_feat['rpm_util']<0.6)]['seconds']
    engine_feat['ft_rpm_util_60pct_s']=engine_feat[(0.60<=engine_feat['rpm_util'])&\
        (engine_feat['rpm_util']<0.7)]['seconds']

    return engine_feat


# def vehicle_wise_weekly_agg(engine_feat_agg,agg_cols):
#     grouped=engine_feat_agg.groupby('vehicle_id')
#     res_df=[]
#     for vehicle_id,df in grouped:

#         temp=df.set_index(pd.DatetimeIndex(df['datetime_pst']))
#         temp=temp.resample("W-Mon")[agg_cols].sum()
#         temp['vehicle_id']=vehicle_id
#         temp['week_start_Date']=temp['datetime_pst'].dt.date
#         res_df.append(temp)

#     return pd.concat(res_df).sort_by(['vehicle_id',"week_start_Date"])

def vehicle_wise_weekly_agg(engine_feat_agg,agg_cols):
    grouped=engine_feat_agg.groupby('vehicle_id')
    res_df=[]
    for vehicle_id,df in tqdm(grouped,desc="combining-result"):
        
        temp=df.set_index(pd.DatetimeIndex(df['datetime_pst']))
        temp=temp.resample("W-Mon")[agg_cols].sum()
        temp['vehicle_id']=vehicle_id
        temp['week_start_date']=temp.index.date
        res_df.append(temp)

    return pd.concat(res_df).sort_values(by=['vehicle_id',"week_start_date"])[ordered_cols]


if __name__=='__main__':
    engine_df=read_files(drive_file_list)
    vehicle_df=pd.read_csv(DIR+"vehicle.csv")
    vc=engine_df['vehicle_id'].value_counts()
    ordered_cols=['vehicle_id','week_start_date','ft_torque_util_60pct_s',
	'ft_torque_util_70pct_s',	'ft_torque_util_80pct_s',	'ft_torque_util_90pct_s',	'ft_horsepower_util_50pct_s',
    	'ft_horsepower_util_60pct_s',	'ft_horsepower_util_70pct_s',	'ft_horsepower_util_80pct_s',
        	'ft_rpm_util_50pct_s','ft_rpm_util_60pct_s']    
    engine_df=time_zone_conversion(engine_df)
    engine_df=add_date(engine_df)
    engine_vehicle_df=merge_vechicle(engine_df,vehicle_df)
    
    engine_feat=engine_feature_creation(engine_vehicle_df)
    engine_feat_agg=feature_aggregations(engine_feat)

    agg_cols=['ft_torque_util_60pct_s','ft_torque_util_70pct_s','ft_torque_util_80pct_s',
    'ft_torque_util_90pct_s','ft_horsepower_util_50pct_s','ft_horsepower_util_60pct_s',
    'ft_horsepower_util_70pct_s','ft_horsepower_util_80pct_s','ft_rpm_util_50pct_s',
    'ft_rpm_util_60pct_s']
    res=vehicle_wise_weekly_agg(engine_feat_agg,agg_cols)
    res.to_csv(DIR+"engine_features.csv",index=False)
#    engine_feat_agg.to_csv(DIR+"engine_features.csv",index=False)













