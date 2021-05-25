# -*- coding: utf-8 -*-

# NOTE: pandas already has a good merge function (pd.merge)
#       We don't need to reproduce it here.
#def _merge(df, right, how='inner', 
#           on=None, left_on=None, right_on=None, 
#           left_index=False, right_index=False, 
#           sort=False, suffixes=('_x', '_y'), 
#           copy=True, indicator=False, validate=None):
#    return df.merge(
#             right, how, on, 
#             left_on, right_on, 
#             left_index, right_index, 
#             sort, suffixes, 
#             copy, indicator, validate)

def _inner_join(
        df, right, 
        on=None, left_on=None, right_on=None, 
        left_index=False, right_index=False, 
        sort=False, suffixes=('_x', '_y'), 
        copy=True, indicator=False, validate=None):
    return df.merge(
        right, 'inner', on, 
        left_on, right_on, 
        left_index, right_index, 
        sort, suffixes, 
        copy, indicator, validate)

def _left_join(
        df, right, 
        on=None, left_on=None, right_on=None, 
        left_index=False, right_index=False, 
        sort=False, suffixes=('_x', '_y'), 
        copy=True, indicator=False, validate=None):
    return df.merge(
        right, 'left', on, 
        left_on, right_on, 
        left_index, right_index, 
        sort, suffixes, 
        copy, indicator, validate)

def _right_join(   
        df, right, 
        on=None, left_on=None, right_on=None, 
        left_index=False, right_index=False, 
        sort=False, suffixes=('_x', '_y'), 
        copy=True, indicator=False, validate=None):
    return df.merge(
        right, 'right', on, 
        left_on, right_on, 
        left_index, right_index, 
        sort, suffixes, 
        copy, indicator, validate)

def _outer_join(
        df, right, 
        on=None, left_on=None, right_on=None, 
        left_index=False, right_index=False, 
        sort=False, suffixes=('_x', '_y'), 
        copy=True, indicator=False, validate=None):
    return df.merge(
        right, 'outer', on, 
        left_on, right_on, 
        left_index, right_index, 
        sort, suffixes, 
        copy, indicator, validate)

def _semi_join(
        left, right, 
        on=None, left_on=None, right_on=None,
        sort=False,
        copy=True):
    """Filtering join.  This will keep the rows in 'left' that 
    have matching join keys in 'right'
    """    
        
    # if 'right_on' is specified, we will rename the keys
    # in the right table to match their counterparts in the
    # left table.
    if right_on is not None:
        
        if left_on is not None:
            on = left_on
               
        # ensure left_on and right_on are both lists (they might just be strings)
        if not isinstance(on, list):
            on = [on]
        if not isinstance(right_on, list):
            right_on = [right_on] 
               
        mapping = {}
        for i in range(len(right_on)):
            mapping[right_on[i]] = on[i]
        right = right.rename(columns=mapping)
    
    elif on is None:
        # if 'on' is not specified, we use the column names that are
        # shared in both data frames:
        on = list(set(left.columns).intersection(set(right.columns)))
    
    right = right.loc[:,on].drop_duplicates()
    return left.merge(right, on=on, sort=sort, copy=copy)
                        
def _anti_join(
        left, right,
        on=None, left_on=None, right_on=None,
        sort=False,
        copy=True):
    """The opposite of semi_join.  This will keep the rows in 'left'
    that DO NOT have matching join keys in 'right'
    """
    
    # if 'right_on' is specified, we will rename the keys
    # in the right table to match their counterparts in the
    # left table.
    if right_on is not None:
        if left_on is not None:
            on = left_on
               
        # ensure left_on and right_on are both lists (they might just be strings)
        if not isinstance(on, list):
            on = [on]
        if not isinstance(right_on, list):
            right_on = [right_on]
               
        mapping = {}
        for i in range(len(right_on)):
            mapping[right_on[i]] = on[i]
        right = right.rename(columns=mapping)

    elif on is None:
        # if 'on' is not specified, we use the column names that are
        # shared in both data frames:
        on = list(set(left.columns).intersection(set(right.columns)))

    # indicator=True creates a new column names _merge with values:
    # (left_only, right_only, both).  We want to keep those where
    # left_only is present.
    right = right.loc[:,on].drop_duplicates()
    df_outer = left.merge(right, how='left', 
                          on=on, sort=sort, copy=copy, 
                          indicator=True)

    df_anti = df_outer[(df_outer._merge == 'left_only')].drop('_merge', axis = 1)
    return df_anti
    