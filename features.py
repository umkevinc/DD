"""
Enrollment Level Features

- Total # of Activity
- Total # of problem
- Total # of video
- Total # of access
- Total # of wiki
- Total # of discussion
- Total # of navigate
- Total # of page close

- Perct of problem
- Perct of video
- Perct of access
- Perct of wiki
- Perct of discussion
- Perct of navigate
- Perct of page close

- total problem - course level avg
- total vedio - course level avg
- total access - course level avg
- total wiki - course level avg
- total discussion - course level avg
- total navigate - course level avg
- total page close - course level avg

- percentile of # of Activity
    - percentile of # of problem
    - percentile of # of vedio
    - percentile of # of access
    - percentile of # of wiki
    - percentile of # of discussion
    - percentile of # of navigate
    - percentile of # of page close

- Total # of unique activity day

- Most activity DOW
- 2nd most activity DOW
- 3rd most activity DOW

TODO:
[filter: First 15 Days]
All Above

[filter: Last 15 Days]
All Above

Difference Begin and End:

- Slope of # of activity

User Level Feature
- # of dropped / # of enrollment


Activity Level Features
- per course max(problem) - person(problem)
"""

"""
Investigation
- How many people in test is also in train
25102.0/80362 = .312 (30% of people in test are also in train)

- How many people in test is also in train has dropped
21791.0/80362 = 0.271

- Course overlap between train and test
Test: 39 Courses
Train: 39 Courses
100% Overlap
"""
import pandas as pd
import numpy as np


"""
Enrollment Level Features
"""
def activity_total_and_percentile(enroll_df=None, log_df=None, dt_df=None):
    """
    Enrollment Level Features - I
     Features:
    - Total # of Activity
    - Total # of problem
    - Total # of video
    - Total # of access
    - Total # of wiki
    - Total # of discussion
    - Total # of navigate
    - Total # of page close
    + 
    Percentile of all above

    Return DataFrame
    Key: enrollment_id, course_id
    """
    big_df = pd.merge(enroll_df, log_df, on='enrollment_id', how='left')
    big_df = pd.merge(big_df, dt_df, on='course_id', how='left')
    act_df = big_df.groupby(['enrollment_id', 'course_id']).event.count().reset_index()
    event_list = log_df.event.unique().tolist()
    for event_name in event_list:
        big_df['e_%s' % event_name] = big_df['event'] == event_name    
        act_df[event_name] = big_df.groupby(['enrollment_id', 'course_id'])['e_%s' % event_name].sum().reset_index()['e_%s' % event_name]

    frames = []
    for k, gp in act_df.groupby('course_id'):
        # Each Course Group
        ndf = pd.DataFrame()
        ndf['enrollment_id'] = gp['enrollment_id']
        ndf['course_id'] = gp['course_id']
        ndf['prctl_total_activity'] = gp.event.rank(pct=True)
        ndf['total_activity'] = gp.event
        for event_name in event_list:
            # Each Event Activity
            # Activity Count
            ndf['total_%s_activity' % event_name] = gp[event_name] 
            # Activity Percentile in Course Group
            ndf['prctl_%s_activity' % event_name] = gp[event_name].rank(pct=True)
        frames.append(ndf)        
    return pd.concat(frames)

def activity_filtered(enroll_df=None, log_df=None, dt_df=None):
    """
    Enrollment Level Features - II

    Return DataFrame
    Key: enrollment_id
    """
    ndf = pd.DataFrame()
    big_df = pd.merge(enroll_df, log_df, on='enrollment_id', how='left')
    big_df = pd.merge(big_df, dt_df, on='course_id', how='left')
    big_df['before_end'] = (big_df['to'] - big_df['time']).astype('m8[D]')
    big_df['after_start'] = (big_df['time'] - big_df['from']).astype('m8[D]')
    big_df['class_len'] = (big_df['to'] - big_df['from']).astype('m8[D]')    

    d_range = [5, 10, 15]
    ndf = pd.DataFrame()
    ndf['enrollment_id'] = enroll_df['enrollment_id']
    for d in d_range:            
        head_df = big_df[big_df.after_start <= d]
        tail_df = big_df[big_df.before_end <= d]    
        
        head_df = pd.DataFrame(head_df.groupby('enrollment_id').event.count()).reset_index()
        head_df.columns = ['enrollment_id', 'total_activity_top_%s' % d]
        head_df
        ndf = pd.merge(ndf, head_df, on='enrollment_id', how='left').fillna(0)

        tail_df = pd.DataFrame(tail_df.groupby('enrollment_id').event.count()).reset_index()
        tail_df.columns = ['enrollment_id', 'total_activity_tail_%s' % d]
        tail_df
        ndf = pd.merge(ndf, tail_df, on='enrollment_id', how='left').fillna(0)
        
        ndf['total_activity_diff_%s' % d] = ndf['total_activity_tail_%s' % d] - ndf['total_activity_top_%s' % d]
    #ndf = ndf.reset_index()
    return ndf



"""
Course Level Features
"""
def avg_activity_per_course_event(enroll_df=None, log_df=None):
    """
    Course Level Features

    Features:
    - Avg. # of Activity
    - Avg. # of problem
    - Avg. # of video
    - Avg. # of access
    - Avg. # of wiki
    - Avg. # of discussion
    - Avg. # of navigate
    - Avg. # of page close

    Return DataFrame
    Key: course_id
    """
    ndf = pd.DataFrame()
    user_df = pd.merge(enroll_df, log_df, on='enrollment_id', how='left')
    user_course_grp = user_df.groupby(['username', 'course_id'])
    ndf['avg_activity'] = user_course_grp.event.count().reset_index().groupby('course_id').event.mean()
    event_list = log_df.event.unique().tolist()
    for event_name in event_list:
        user_course_grp = user_df[user_df['event']==event_name].groupby(['username', 'course_id'])
        ndf['avg_activity_'+event_name] = user_course_grp.event.count().reset_index().groupby('course_id').event.mean()
    ndf = ndf.reset_index()
    return ndf
