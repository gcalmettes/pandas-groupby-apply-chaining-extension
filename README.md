# `gcGroupbyExtension`: chain `.apply` methods on Pandas groupby object

## Install
The extension is available on [PyPi](https://pypi.org/project/gcGroupbyExtension-gcalmettes/)
```
pip install gcGroupbyExtension-gcalmettes
```

(Or if you do not want to install the package in your python distribution, just download this repo and place the `gcGroupbyExtension` folder in the folder you're running your python script/notebook in.)

## Import
Once installed, the extension can be imported via:
```
import gcGroupbyExtension
```

## What problems does this extension try to solve?
[Pandas](https://pandas.pydata.org) provides both the [`.pipe`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.core.groupby.GroupBy.pipe.html) and [`.apply`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.core.groupby.GroupBy.apply.html) methods to work on its [groupby](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html) object.
The main difference between `.pipe` and `.apply` in the groupby context is that you have access to the entire scope of the groupby object (each group) with `.pipe`, while you only have access to the subcomponents scope (in the context of a groupby the subcomponents are slices of the dataframe that called groupby where each slice is a dataframe itself. This is analogous for a series groupby.)
1) The `.pipe` method can be chained, while the `.apply` method can't.
2) You can use the [`.agg`](https://pandas.pydata.org/pandas-docs/version/0.22/generated/pandas.core.groupby.DataFrameGroupBy.agg.html) method to limit the application of the functions on particular columns of the groups, but it is cumbersome to apply specific functions independantly on only a selection of the groups.
3) There is no easy way to construct independant pipelines of functions for each group.

This extension provides this capability.

## What does this extension actually do?
This extension allows to construct a pipeline of functions to be applied independently on the groups of a groupby object. The functions/transformations to be applied can be the same for all the groups or scoped to (a) specific group(s).

**Details:**
This library registers a [custom accessor](https://pandas.pydata.org/pandas-docs/stable/development/extending.html) on pandas DataFrame and Series objects.
The methods of this extension are registered under the `gc` namespace.

See the [DEMO notebook](https://github.com/gcalmettes/pandas-groupby-apply-chaining-extension/blob/master/demo.ipynb) for details. 

## Care to show the syntax?
Sure! See the [DEMO notebook](https://github.com/gcalmettes/pandas-groupby-apply-chaining-extension/blob/master/demo.ipynb) for more details, but basically, you can do things like this:

```
(df.gc.groupby("nameOfColumn")
    .resetIndex() # this is a special method baked in
    .apply(lambda x: x * 5, lambda x: x + x.iloc[3]) # accepts multiple functions
    .apply(mySpecialFunction, onlyGroups=['group1']) # limit the function to specific group(s)
    .apply(lambda x: x - x.mean(), ignoreGroups=['group4', 'group6']) # limit the function to specific group(s)
    .apply(lambda x: x.std(axis=1))
    .concat(axis=0, multiIndex=None).plot()
)
```
