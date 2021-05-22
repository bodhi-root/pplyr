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

#def _semi_join:
#def _anti_join: