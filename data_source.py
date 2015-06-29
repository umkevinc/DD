import pandas as pd
import numpy as np


# Main Dataframes
def get_enrollment_df(ftype):
    """
    Get Enrollment Dataframe (enrollment_<type>.csv)
    """
    assert ftype=='train' or ftype=='test'
    enroll_df = pd.read_csv('data/%s/enrollment_%s.csv' % (ftype, ftype))
    return enroll_df


def get_log_df(ftype):
    """
    Get Log Dataframe (log_<type>.csv)
    """
    assert ftype=='train' or ftype=='test'
    log_df = pd.read_csv('data/%s/log_%s.csv' % (ftype, ftype))
    log_df['time'] = pd.to_datetime(log_df['time'])
    log_df['action_date'] = log_df.time.apply(lambda x: x.date())
    log_df['action_dow'] = log_df['time'].apply(lambda x: x.weekday())
    return log_df


def get_labels_df():
    """
    Get Trainning Labels Dataframe (truth_train.csv)
    """
    labels_df = pd.read_csv('data/train/truth_train.csv', header=None)
    return labels_df


# Reference Files
def get_date_df():
    """
    Get Date Dataframe (date.csv)
    """
    dt_df = pd.read_csv('data/date.csv')
    dt_df['from'] = pd.to_datetime(dt_df['from'])
    dt_df['from_year'] = dt_df['from'].apply(lambda x: x.year)
    dt_df['from_month'] = dt_df['from'].apply(lambda x: x.month)
    dt_df['to'] = pd.to_datetime(dt_df['to'])
    dt_df['to_year'] = dt_df['from'].apply(lambda x: x.year)
    dt_df['to_month'] = dt_df['from'].apply(lambda x: x.month)
    return dt_df

def get_obj_df():
    """
    Get Object Dataframe (object.csv)
    """
    obj_df = pd.read_csv('data/object.csv')
    obj_df = obj_df.drop_duplicates()[['course_id', 'module_id', 'category', 'start']]  
    obj_df['start'] = pd.to_datetime(obj_df[obj_df['start'] != 'null']['start'])
    return obj_df


def get_sample_submission():
    sample_sub_df = pd.read_csv('data/sampleSubmission.csv', header=None)
    return sample_sub_df

# Joined Dataframe
def get_enroll_log_df(log_df, enroll_df, dt_df, obj_df):
    big_df = pd.merge(enroll_df, log_df, on='enrollment_id', how='left')
    big_df = pd.merge(big_df, dt_df, on='course_id', how='left')
    big_df = pd.merge(big_df, obj_df, left_on=['course_id', 'object'], right_on=['course_id', 'module_id'], how='left')
    print big_df.shape
    big_df['before_end'] = (big_df['to'] - big_df['time']).astype('m8[D]')
    big_df['after_start'] = (big_df['time'] - big_df['from']).astype('m8[D]')
    big_df['class_len'] = (big_df['to'] - big_df['from']).astype('m8[D]')
    big_df['after_module_released'] = (big_df['time'] - big_df['start']).astype('m8[D]')
    return big_df




