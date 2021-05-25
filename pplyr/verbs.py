# -*- coding: utf-8 -*-

import types
#import numpy as np
import pandas as pd

def __regroup(df, dfg):
    """calls df.groupby() in a way that will mimic the same way
    dfg was grouped
    """
    return df.groupby(
              by=dfg.keys, 
              level=dfg.level, 
              as_index=dfg.as_index, 
              sort=dfg.sort, 
              group_keys=dfg.group_keys, 
              observed=dfg.observed, 
              dropna=dfg.dropna)    

def __remove_last_index(df, drop=False, inplace=False):
    """Removes the last column of a dataframe index.  This is useful
    because methods like apply() sometimes add a new level to our
    index and we frequently want to get rid of that.
    """
    return df.reset_index(
        level=len(df.index.names)-1, 
        drop=drop, 
        inplace=inplace)

def _select(df, cols=None, start=None, end=None):
    """Select specific columns from a data.frame.
    
    df.pipe(select, ["col1", "col2"])
    df.pipe(select, lambda x: x.endswith("_suffix"))
    df.pipe(select, start="col1", end="col3")
    """
    
    # if we have a grouped DataFrame we'll apply the function
    # to the underlying obj and then regroup it as it was before.
    if isinstance(df, pd.core.groupby.DataFrameGroupBy):
        df_new = _select(df.obj, cols, start, end)
        return __regroup(df_new, df)
    
    if cols is not None:
        if isinstance(cols, types.FunctionType):
            keep_cols = df.columns[df.columns.map(cols)]
            return df[keep_cols]
        else:
            keep_cols = [col if isinstance(col,str) else df.columns[int(col)] for col in cols]
            return df[keep_cols]
    else:
        if (start is not None) and (end is not None):
            cols = list(df.columns)
            
            if isinstance(start, str) and isinstance(end, str):
                idx_start = cols.index(start)
                idx_end = cols.index(end) + 1
            else:
                idx_start = int(start)
                idx_end = int(end) + 1
            
            keep_cols = cols[idx_start:idx_end]
            return df[keep_cols]
    
    raise Exception("select() must define either 'cols' or both 'start' and 'end'")

def _drop(df, cols=None, start=None, end=None):
    """Drop columns from a data.frame.
    
    df.pipe(drop, ["col1", "col2"])
    df.pipe(drop, lambda x: x.endswith("_suffix"))
    df.pipe(drop, start="col1", end="col3")
    """
    # if we have a grouped DataFrame we'll apply the function
    # to the underlying obj and then regroup it as it was before.
    if isinstance(df, pd.core.groupby.DataFrameGroupBy):
        df_new = _drop(df.obj, cols, start, end)
        return __regroup(df_new, df)
    
    if cols is not None:
        if isinstance(cols, types.FunctionType):
            drop_cols = df.columns[df.columns.map(cols)]
            return df.drop(drop_cols, axis=1)
        else:
            return df.drop(cols, axis=1)
    else:
        if (start is not None) and (end is not None):
            cols = list(df.columns)
            idx_start = cols.index(start)
            idx_end = cols.index(end) + 1
            drop_cols = cols[idx_start:idx_end]
            return df.drop(drop_cols, axis=1)
    
    raise Exception("drop() must define either 'cols' or both 'start' and 'end'")

def __col_index(df, value):
    """Convert a column value to an index.  The value can either be an index
    already or a string indicating a column name.
    """
    if isinstance(value, str):
        return list(df.columns.values).index(value)
    else:
        return int(value)
    
def __col_name(df, value):
    """Converts a column value to a name.  The value can either be a name
    already of an index of a column.
    """
    if isinstance(value, str):
        return value
    else:
        return df.columns.values[value]

def _relocate(df, cols=None, start=None, end=None, before=None, after=None):
    """Move columns.  The columns to move can be specified by either:
    1. The 'cols' parameter which accepts an array
    2. The 'start' and 'end' parameters that indicate start and end columns
    By default, columns will be moved to the front of the DataFrame.  
    Alternately, the user can specify where they want to place them using
    the 'before' or 'after' parameters.
    """
    # if we have a grouped DataFrame we'll apply the function
    # to the underlying obj and then regroup it as it was before.
    if isinstance(df, pd.core.groupby.DataFrameGroupBy):
        df_new = _relocate(df.obj, cols, start, end, before, after)
        return __regroup(df_new, df)
    
    # convert whatever they gave us in 'cols', 'start', and 'end' into
    # a cols list of named columns.
    if cols is not None:
        if isinstance(cols, str):
            cols = [cols]
        else:
            cols = [__col_name(df, col) for col in cols]
    else:
        start_idx = __col_index(df, start)
        end_idx = __col_index(df, end) + 1
        cols = list(df.columns)[start_idx:end_idx]
        
    if (before is None) and (after is None):
        before = 0
    
    new_cols = [col for col in df.columns.values if col not in cols]
    if before == 0:
        cols.extend(new_cols)
        new_cols = cols
    elif after == -1:
        new_cols.extend(cols)
    elif before is not None:
        before = __col_name(df, before)
        idx = new_cols.index(before)
        new_cols[idx:idx] = cols
    elif after is not None:
        after = __col_name(df, after)
        idx = new_cols.index(after) + 1
        new_cols[idx:idx] = cols
    else:
        raise Exception("This should not happen")
    
    return df.loc[:,new_cols]

def _rename(df, **kvargs):
    """Rename columns
    
    df.pipe(rename, new_col="col1")
    """
    # if we have a grouped DataFrame we'll apply the function
    # to the underlying obj and then regroup it as it was before.
    if isinstance(df, pd.core.groupby.DataFrameGroupBy):
        df_new = _rename(df.obj, **kvargs)
        return __regroup(df_new, df)
    
    # invert the column mappings (convert integers to column names)
    inv_map = {}
    for k, v in kvargs.items():
        if not isinstance(v, str):
            v = df.columns[int(v)]
        inv_map[v] = k
    
    return df.rename(columns=inv_map)

def _rename_with(df, func):
    """Renames columns using a function that is applied to each
    column name.  Example:
        
        df.pipe(rename, lambda x: x.upper())
    """
    # if we have a grouped DataFrame we'll apply the function
    # to the underlying obj and then regroup it as it was before.
    if isinstance(df, pd.core.groupby.DataFrameGroupBy):
        df_new = _rename_with(df.obj, func)
        return __regroup(df_new, df)
    
    return df.rename(columns=func)

def _filter(df, f_filter):
    """Filters the rows of a data frame.  This is done using a function 
    applied to the data frame. Usually, this function will return a vector
    of True/False values indicating the rows to include/exclude (although
    other indexing methods are supported as well).
    
    df.pipe(_filter, lambda x: (x.col1 == "value1") & (x.col2 == "value2"))
    """
    if isinstance(df, pd.core.groupby.DataFrameGroupBy):
        df_new = df.apply(lambda x: _filter(x, f_filter))
        df_new.reset_index(drop=True, inplace=True)
        return __regroup(df_new, df)
    else:
        idx = f_filter(df)
        
        # if only a single True/False value is returned, create a Series
        # that repeats this value and aligns with the DataFrame index.
        # This is helpful for grouped filter operations.
        if isinstance(idx, bool):
            idx = pd.Series([idx] * len(df), index=df.index)
        
        return df[idx]

def _slice(df, *argv):
    """Returns selected rows of the data frame.  If *argv contains only
    1 value we return a single row with this index.  Otherwise, the
    contents of *argv are passed to python's native 'slice(start, end, by)'
    function.
    
    df.pipe(_slice, 0)
    df.pipe(_slice, 0, 1)
    """
    if len(argv) == 0:
        raise Exception("slice() what?  Come on, I need some arguments!")
    
    if isinstance(df, pd.core.groupby.DataFrameGroupBy):
        df_new = df.apply(lambda x: _slice(x, *argv))
        df_new.reset_index(drop=True, inplace=True)
        return df_new
    else:
        if len(argv) == 1:
            return df[slice(argv[0], argv[0]+1)]
        else:
            return df[slice(*argv)]

def _slice_head(df, n=None, prop=None):
    """Returns first n rows or the top round(prop * len(df)) rows
    from the DataFrame.
    """
    if isinstance(df, pd.core.groupby.DataFrameGroupBy):
        df_new = df.apply(lambda x: _slice_head(x, n, prop))
        df_new.reset_index(drop=True, inplace=True)
        return df_new
    else:
        if prop is None and n is None:
            n = 5
        elif n is None:
            n = round(len(df) * prop)
        return df[slice(0, n)]

def _slice_tail(df, n=None, prop=None):
    """Returns last n rows or the last round(prop * len(df)) rows
    from the DataFrame.
    """
    if isinstance(df, pd.core.groupby.DataFrameGroupBy):
        df_new = df.apply(lambda x: _slice_tail(x, n, prop))
        df_new.reset_index(drop=True, inplace=True)
        return df_new
    else:
        if prop is None and n is None:
            n = 5
        elif n is None:
            n = round(len(df) * prop)
        return df[slice(len(df)-n, len(df))]

def _slice_sample(df, n=None, prop=None, weight_by=None, replace=False):
    """Returns a random sample of rows.  This is an alias for
    DataFrame.sample() with slight parameter name changes.
    """
    if isinstance(df, pd.core.groupby.DataFrameGroupBy):
        df_new = df.apply(lambda x: _slice_sample(x, n, prop))
        df_new.reset_index(drop=True, inplace=True)
        return df_new
    else:
        return df.sample(n=n, frac=prop, replace=replace, weights=weight_by)

def _slice_max(df, by, n=None, prop=None):
    """Arranges the data frame by the criteria in 'by' (descending) and 
    returns the top rows.
    """
    if isinstance(df, pd.core.groupby.DataFrameGroupBy):
        df_new = df.apply(lambda x: _slice_max(x, by, n=n, prop=prop))
        df_new.reset_index(drop=True, inplace=True)
        return df_new
    else:
        df_sorted = df.pipe(_arrange, by, ascending=False)
        return _slice_head(df_sorted, n=n, prop=prop)
    
def _slice_min(df, by, n=None, prop=None):
    """Arranges the data frame by the criteria in 'by' (ascending) and
    returns the top rows.
    """
    if isinstance(df, pd.core.groupby.DataFrameGroupBy):
        df_new = df.apply(lambda x: _slice_min(x, by, n=n, prop=prop))
        df_new.reset_index(drop=True, inplace=True)
        return df_new
    else:
        df_sorted = df.pipe(_arrange, by, ascending=True)
        return _slice_head(df_sorted, n=n, prop=prop)

def _arrange(df, by, ascending=True, inplace=False, kind='quicksort', na_position='last', ignore_index=False, key=None):
    """Arrange rows of the data frame.  This is just a convenience
    method for DataFrame.sort_values().
    """
    if isinstance(df, pd.core.groupby.DataFrameGroupBy):
        df_new = df.apply(lambda x: _arrange(x, 
                              by, 
                              ascending=ascending, 
                              inplace=inplace, 
                              kind=kind, 
                              na_position=na_position, 
                              ignore_index=ignore_index, 
                              key=key))
        df_new.reset_index(drop=True, inplace=True)
        return __regroup(df_new, df)
    else:
        return df.sort_values(by, 
                              ascending=ascending, 
                              inplace=inplace, 
                              kind=kind, 
                              na_position=na_position, 
                              ignore_index=ignore_index, 
                              key=key)


def _mutate(df, **kvargs):
    """Create new columns or modify existing ones.  This is a simple alias
    for DataFrame.assign()."""
    if isinstance(df, pd.core.groupby.DataFrameGroupBy):
        df_new = df.apply(lambda x: x.assign(**kvargs))
        df_new.reset_index(drop=True, inplace=True)
        return __regroup(df_new, df)
    else:
        return df.assign(**kvargs)

def _transmute(df, **kvargs):
    """Create new columns of modify existing ones (similar to mutate()).
    Any columns not defined in this section will be dropped.
    """
    if isinstance(df, pd.core.groupby.DataFrameGroupBy):
        #df_new = df.apply(lambda x: _transmute(x, **kvargs))
        df_new = df.apply(lambda x: x.assign(**kvargs))
        df_new = _select(df_new, list(kvargs.keys()))
        __remove_last_index(df_new, drop=True, inplace=True)
        df_new.reset_index(drop=False, inplace=True)
        return __regroup(df_new, df)
    else:
        df_new = df.assign(**kvargs)
        return _select(df_new, list(kvargs.keys()))

def _summarise(df, **kvargs):
    """Summarise a data frame, creating a new 1-row DataFrame with the
    desired columns.  If the DataFrame is grouped we return 1 row for
    each group and join these together.
    """
    if isinstance(df, pd.core.groupby.DataFrameGroupBy):
        # apply _summarise to each group:
        df_new = df.apply(lambda x: _summarise(x, **kvargs))
        # drop the index we added (which is always zero and is unnamed)
        __remove_last_index(df_new, drop=True, inplace=True)
        df_new.reset_index(drop=False, inplace=True)
        return df_new
    else:
        new_cols = {}
        for k, v in kvargs.items():
            new_cols[k] = pd.core.common.apply_if_callable(v, df)
        return pd.DataFrame(new_cols, index=[0])
    
def _ungroup(df):
    """Removes any grouping on the given DataFrame.  If the data
    frame is a DataFrameGroupBy this will convert it back to a
    regular DataFrame object.  If it is already a DataFrame it will
    reset and drop the index.
    """
    if isinstance(df, pd.core.groupby.DataFrameGroupBy):
        #return df.apply(lambda x: x)
        return df.obj  # original grouped object
    elif isinstance(df, pd.core.frame.DataFrame):
        return df.reset_index(drop=True)
    
def _group_by(df, by=None, level=None, as_index=True, 
              sort=True, group_keys=True, observed=False, 
              dropna=True):
    """Convenience method for DataFrame.groupby() with some additional
    logic to handle objects that are already grouped and cases where
    a simply groupby() might throw an error."""
    if isinstance(df, pd.core.groupby.DataFrameGroupBy):
        df = _ungroup(df)
    return df.groupby(
        by=by, 
        level=level, 
        as_index=as_index,
        sort=sort,
        group_keys=group_keys,
        observed=observed,
        dropna=dropna)

def _distinct(df, subset=None):
    """Returns unique rows in a DataFrame.  This just calls
    DataFrame.drop_duplicates()
    """
    return df.drop_duplicates(subset=subset)

def __get_keys(keys):
    """Returns grouping keys as a list.  This is either a single
    object or a list/tuple of keys.  We have to handle them differently
    because a single-object string key has functions like len()
    that will make it behave like a list and chop it into its
    character pieces.
    """
    if isinstance(keys, list) or isinstance(keys, tuple):
        return list(keys)
    else:
        return [keys]

def _tally(df, sort=False, name="n"):
    """Returns a count of rows in a DataFrame.  This is more useful when
    applied to grouped DataFrames as a count will be returned for each
    group.
    """
    if isinstance(df, pd.core.groupby.DataFrameGroupBy):
        columns = __get_keys(df.keys)
        if (name in columns):
            raise Exception("A column named '{}' already exists. Please specify another 'name' for tally".format(name))
        columns.append("n")
        
        # quick count using 'groups' property:
        rows = []
        for key, indices in df.groups.items():
            row = __get_keys(key)
            row.append(len(indices))
            rows.append(row)

        df_new = pd.DataFrame(rows, columns=columns)
        if sort == True:
            df_new.sort_values(name, inplace=True, ascending=False)
        return df_new
    else:
        return pd.DataFrame({name: len(df)}, index=[0])
    
def _pull(df, col=-1):
    """Returns a column from the DataFrame.  The selector can be a 
    column name (string) or an integer indicating the column index.
    A negative number indicates a location relative to the end of the
    DataFrame.  The default (-1) returns the last column, assuming
    that this is the one you created most recently.
    """
    if isinstance(df, pd.core.groupby.DataFrameGroupBy):
        return _pull(df.obj)
    else:
        if isinstance(col, str):
            return df[col]
        else:
            if col < 0:
                col = col + len(df.columns)
            return df.iloc[:,col]
