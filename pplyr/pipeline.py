# File: pipeline.py
#
# NOTE: I tried a long time to make a 'pipe' operator similar to
# dfply or https://github.com/JulienPalard/Pipe/blob/master/pipe.py.
# This would use code like the following:
#
# --- start code --------------------------------------------------------------
#
# class Pipe(object):
#    """Function decorator that allows the function to be preceded
#    by a right shift '>>' operator.  This allows us to take a dplyr-type
#    function of the form: 
#       
#        @Pipe
#        def _func(df, *argv, **kvargs)
#        
#    and invoke it with:
#        
#        df >> _func(*argv, **kvargs)
#    
#    This is an alternative to using the 'pipe' method on the dataframe:
#    
#        df.pipe(_func, *argv, **kvargs)
#    """
#    
#     def __init__(self, function):
#        self.function = function
#        functools.update_wrapper(self, function)
#
#    def __rrshift__(self, other):
#        return self.function(other)
#
#    def __call__(self, *args, **kwargs):
#        return Pipe(lambda x: self.function(x, *args, **kwargs))
# 
# --- end code ----------------------------------------------------------------   
#
# And this did in fact work.  You could create pipes such as:
#
#     df >> _head(3) >> \
#        _head(1)
#
# but you would no longer be able to run these functions with:
# 
#     df.pipe(_head, 3)
# 
# or:
#    
#     _head(df, 3)
#
# You were basically forced to pick one or the other.  The new method
# takes advantage of the fact that chained method calls can be used 
# instead of a chaining operator.  This means we can create a pipeline
# with:
#
#    p = pipeline() \
#        .head(3) \
#        .tail(1)
#
# The pipeline can be evaluated either as p(df) or df.pipe(p).
# Basically, all we are doing is decorating the DataFrame object with
# dplyr-like methods that are facades for the pandas operations.
# 
# The only downside of this approach is that our ecosytem becomes
# a bit closed.  No one else can define a new function that can be
# used in our pipeline (not without doing the pipe() )

import pandas as pd

from .merge import (
    _inner_join,
    _outer_join,
    _left_join,
    _right_join
)

from .groups import (
    _group_walk,
    _group_map,
    _group_modify
)

from .verbs import (_select, _drop, _rename, _filter,
                    _slice, _slice_head, _slice_tail, _slice_sample,
                    _slice_max, _slice_min,
                    _arrange, _mutate, _transmute, _summarise,
                    _ungroup, _group_by,
                    _distinct, _tally)
        
class pipeline:
    
    def __init__(self):
        self.chained_pipes = []
    
    def __call__(self, df):
        for p in self.chained_pipes:
            df = p(df)
        return df
     
    def pipe(self, f, *argv, **kvargs):
        self.chained_pipes.append(lambda df: f(df, *argv, **kvargs))
        return self
    
    def select(self, *argv, **kvargs):
        return self.pipe(_select, *argv, **kvargs)
    
    
    def drop(self, *argv, **kvargs):
        return self.pipe(_drop, *argv, **kvargs)
    
    def rename(self, *argv, **kvargs):
        return self.pipe(_rename, *argv, **kvargs)
    
    def filter(self, *argv, **kvargs):
        return self.pipe(_filter, *argv, **kvargs)
    
    def slice(self, *argv, **kvargs):
        return self.pipe(_slice, *argv, **kvargs)
    
    def slice_head(self, *argv, **kvargs):
        return self.pipe(_slice_head, *argv, **kvargs)
    
    def head(self, *argv, **kvargs):
        return self.pipe(_slice_head, *argv, **kvargs)
    
    def slice_tail(self, *argv, **kvargs):
        return self.pipe(_slice_tail, *argv, **kvargs)
    
    def tail(self, *argv, **kvargs):
        return self.pipe(_slice_tail, *argv, **kvargs)
    
    def slice_sample(self, *argv, **kvargs):
        return self.pipe(_slice_sample, *argv, **kvargs)
    
    def slice_max(self, *argv, **kvargs):
        return self.pipe(_slice_max, *argv, **kvargs)
    
    def slice_min(self, *argv, **kvargs):
        return self.pipe(_slice_min, *argv, **kvargs)
    
    def arrange(self, *argv, **kvargs):
        return self.pipe(_arrange, *argv, **kvargs)
    
    def mutate(self, *argv, **kvargs):
        return self.pipe(_mutate, *argv, **kvargs)
    
    def transmute(self, *argv, **kvargs):
        return self.pipe(_transmute, *argv, **kvargs)
    
    def summarise(self, *argv, **kvargs):
        return self.pipe(_summarise, *argv, **kvargs)
    
    def summarize(self, *argv, **kvargs):
        return self.pipe(_summarise, *argv, **kvargs)
    
    def ungroup(self, *argv, **kvargs):
        return self.pipe(_ungroup, *argv, **kvargs)
    
    def groupby(self, *argv, **kvargs):
        return self.pipe(_group_by, *argv, **kvargs)
    
    def group_by(self, *argv, **kvargs):
        return self.pipe(_group_by, *argv, **kvargs)
    
    def distinct(self, *argv, **kvargs):
        return self.pipe(_distinct, *argv, **kvargs)
    
    def tally(self, *argv, **kvargs):
        return self.pipe(_tally, *argv, **kvargs)
    
    # merge.py
    
    def merge(self, *argv, **kvargs):
        return self.pipe(pd.merge, *argv, **kvargs)
    
    def inner_join(self, *argv, **kvargs):
        return self.pipe(_inner_join, *argv, **kvargs)
    
    def outer_join(self, *argv, **kvargs):
        return self.pipe(_outer_join, *argv, **kvargs)
    
    def left_join(self, *argv, **kvargs):
        return self.pipe(_left_join, *argv, **kvargs)
    
    def right_join(self, *argv, **kvargs):
        return self.pipe(_right_join, *argv, **kvargs)
    
    # groups
    
    def group_walk(self, *argv, **kvargs):
        return self.pipe(_group_walk, *argv, **kvargs)
    
    def group_map(self, *argv, **kvargs):
        return self.pipe(_group_map, *argv, **kvargs)
    
    def group_modify(self, *argv, **kvargs):
        return self.pipe(_group_modify, *argv, **kvargs)
    
    # DataFrame functions:
        
    def reset_index(self, level=None, drop=False, col_level=0, col_fill=''):
        def func(df):
            return df.reset_index(
                level=level, 
                drop=drop, 
                col_level=col_level, 
                col_fill=col_fill)
        return self.pipe(func)
  
    

# Alternate way to add methods:
#
#def __wrap_method(func):
#    def wrapped(self, *argv, **kvargs):
#        return self.pipe(func, *argv, **kvargs)
#    
#    wrapped.__doc__ = func.__doc__
#    return wrapped
#
#pipeline.select2 = __wrap_method(_select)
    
