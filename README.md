# pplyr

## Overview

This is yet-another library for making python feel a bit more like
dplyr and the tidyverse with its assortment of useful methods.
Unlike other libraries (see 'dplyr') the goal of this library is to
maintain a "pythonic" feel.  In some cases we even retain the original
"pandas" syntax.

There are two ways to use this library:

1. Through functions such as "select", "filter" that take a DataFrame as
   their first parameter (and can thus be used with "DataFrame.pipe()")
2. Using a "pipeline" object that allows methods to be chained together
   before applying the pipeline to a DataFrame.
   
An example of the first usage would be this:

```
import pplyr

df2 = (df
  .pipe(pplyr.filter, lambda x: x.eye_color == "blue")
  .pipe(pplyr.select, "col1", "col2")
  .groupby("sex")
  .pipe(pplyr.summarise,
        n = lambda x: len(x),
        avg_height = lambda x: x.height.mean()
  )
)
```

The equivalent operations expressed as a pipeline are:

```
from pplyr import pipeline

df2 = df.pipe(pipeline()
  .filter(lambda x: x.eye_color == "blue")
  .select(["col1", "col2"])
  .groupby("sex")
  .summarise(
    n = lambda x: len(x),
    avg_height = lambda x: x.height.mean()
  )
)
```

Pipelines can also be saved as a separate object that can be applied
to a DataFrame one or more times:

```
p = pipeline()
  .filter(lambda x: x.eye_color == "blue")
  .select(["col1", "col2"])
  .groupby("sex")
  .summarise(
    n = lambda x: len(x),
    avg_height = lambda x: x.height.mean()
  )
```

and then either:

```
df2 = df.pipe(p)
```

or:

```
df2 = p(df)
```

Notice the slight changes to the traditional dplyr syntax to make
it more 'pythonic' or 'pandas' compatible:

* 'select' takes an array of columns instead of "select('col1', 'col2')"
* 'groupby' instead of 'group_by' (although we actually have 'group_by' as an alias)
* lambda functions are used instead of trying to reproduce R's formula objects

## Merges and Joins

Pandas "merge" function is nearly identical to dplyr's.  As such, we stick
with the pandas vocabulary in this case.  We don't define our own merge
function. Instead, we just use 'pd.merge' which looks like this:

```
pd.merge(left, right, how='inner', 
           on=None, left_on=None, right_on=None, 
           left_index=False, right_index=False, 
           sort=False, suffixes=('_x', '_y'), 
           copy=True, indicator=False, validate=None)
```

Some changes to note for those coming from dplyr:

* 'on' is used instead of 'by'
* 'left_on' and 'right_on' replace 'x.by' and 'y.by'
* 'sort' defaults to False (while python's merge defaults to 'True')
* 'suffixes' are ('_x', '_y') instead of ('.x', '.y')

These are very intuitive changes.  As such, we don't expect R users to have
much problem with the pandas syntax.  We do implement the following convenience
functions:

* inner_join
* left_join
* right_join
* outer_join

All of these call pd.merge with the 'how' parameter set to 'inner', 'left', 
'right', or 'outer'.  Notice that we use 'outer_join' instead of 'full_join'.

## Additional Notes

Some places where our methods add a lot of value are with functions like
'select' and 'filter'.  While there are ways to do these operations in 
pandas, they aren't done with traditional methods.  This means you can't
introduce these operations in the middle of a method chain (as far as I know).

Instead of:

```
result = df.select("name", "sex").filter(lambda x: x.sex == "female")
```

You'd instead have to do:

```
df2 = df[["name","sex"]]
result = df2[df2.sex == "female"]
```

Also, the 'lambda' filter feels cleaner than having to type the entire
dataframe name in the filter criteria.  It lets us do:

```
df_new_students.filter(lambda x: (x.sex == "female") & (x.eye_color == "blue"))
```

instead of:

```
df_new_students[(df_new_students.sex == "female") & (df_new_students.eye_color == "blue")]
```

This saves us some keystrokes and promotes readability when the data frame
name is long.

NOTE: Once more I'll caveat this by saying I'm new to pandas.  Maybe there
are ways to do the operations above in a more elegant way.  If there are,
I would love to know how.
