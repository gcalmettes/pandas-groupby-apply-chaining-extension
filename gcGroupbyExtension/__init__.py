import pandas as pd
import numpy as np
import json
from functools import reduce

@pd.api.extensions.register_dataframe_accessor("gc")
@pd.api.extensions.register_series_accessor("gc")
class GroupByChainedApply(object):
    def __init__(self, pandas_obj):
        self._validate(pandas_obj)
        self._obj = self._convertToDataFrame(pandas_obj)
        self._pipedFunctions = []
        
    def __repr__(self):
        return "<CustomGroupBy object>"
    
    def __call__(self, renameCol="Value"):
        self._obj = self._rename(self._obj, renameCol)
        return self

    @staticmethod
    def _validate(obj):
        if not isinstance(obj, (pd.DataFrame, pd.Series)):
            raise AttributeError("The object must be a pandas DataFrame or Series.")
    
    @staticmethod
    def _convertToDataFrame(obj, colName="Value"):
        if isinstance(obj, pd.Series):
            outDf = pd.DataFrame({colName: obj.values.ravel()}, index=obj.index)
        else:
            outDf = obj.copy()
        return outDf
    
    @staticmethod
    def _rename(obj, colName):
        outDf = obj
        if len(obj.columns) == 1:
            outDf = pd.DataFrame({colName: obj.values.ravel()}, index=obj.index)
        return outDf
    
    @staticmethod
    def _validatePipelineObject(obj):
        if not isinstance(obj, (pd.core.groupby.generic.DataFrameGroupBy, pd.core.groupby.generic.SeriesGroupBy)):
            raise TypeError("A groupby operation has to be applied before a pipeline can be executed.")
    
    @staticmethod
    def _pipe(*funcs):
        '''Combine a list of functions with a pipe operator'''
        return lambda x: reduce(lambda f, g: g(f), list(funcs), x)
    
    @staticmethod
    def _resetIndex(df, resetPosition=0, handleFuture=True):
        '''Reset index using the given position as zero value'''
        isIndexTimeType = np.issubdtype(df.index, np.datetime64)
        func = lambda x: x - x[resetPosition]
        if isIndexTimeType and handleFuture:
            return df.set_index(keys=pd.to_datetime(func(df.index)))
        else:
            return df.set_index(keys=func(df.index))

    @staticmethod
    def _getIdxFrom(idx, idxList, axis):
        if idx in idxList and type(idx) == type(idxList[0]):
            # need the type checking as pd.to_datetime("1970-01-01 00:05:01") == 0
            pos = list(idxList).index(idx)
        elif not isinstance(idx, int):
            raise TypeError(f"{idx} is not a valid {'index' if axis=='index' else 'columns'} identifier.")
        elif isinstance(idx, int):
            pos = idx
        else:
            raise ValueError("An error occured when retrieving the index.")
        return pos

    @staticmethod
    def _generateIndexNames(groupbyObject, axis='columns', multiIndex='hierarchy', sep='|'):
        axisNameDict = {0: 'index', 1: 'columns'}
        axis = axis if axis and not isinstance(axis, int)\
               else axisNameDict.get(axis) if axisNameDict.get(axis)\
               else 'columns'
        if multiIndex:
            if multiIndex == 'join':
                # create flat index and join group name at end of each column with separator in between
                newNames = map(lambda x: map(lambda y: f"{y}{sep}{x[0]}", getattr(x[1], axis, [0])), groupbyObject)
                newNames = [name for subset in newNames for name in subset]
            elif multiIndex == 'hierarchy':
                # create multi index hierarchy for the columns
                newNames = map(lambda x: map(lambda y: (x[0],y), getattr(x[1], axis, [0])), groupbyObject)
                newNames = pd.MultiIndex.from_tuples([name for subset in newNames for name in subset])
            else:
                raise ValueError("The provided multiIndex argument is not of 'hierarchy' | 'join' ") 
        else:
            newNames = map(lambda x: map(getattr(x[1], axis, [0])), groupbyObject)
        return newNames

    def _clearPipeline(self):
        self._pipedFunctions = []
        return self
                            
    def _execute(self, df, index=0, column=None, operation="subtract"):
        '''Apply a DataFrame-wise operation using the provided row (index) or column'''
        numericCols = df.select_dtypes(include=[np.number]).columns
        if column is None: # index-based indexing
          axis = 'index'
          pos = self._getIdxFrom(index, df.index, axis)
        else: # column-based indexing
          axis = 'columns'
          pos = self._getIdxFrom(column, numericCols, axis)
    
        # operation types
        if operation == "subtract":
            func = lambda x: x - x.iloc[pos]
        elif operation == "add":
            func = lambda x: x + x.iloc[pos]
        elif operation == "multiply":
            func = lambda x: x * x.iloc[pos]
        elif operation == "divide":
            func = lambda x: x / x.iloc[pos]
        else:
            raise ValueError("The provided operation to perform is not of subtract/add/multiply/divide")
        df.loc[:, numericCols] = df.loc[:, numericCols].apply(func, axis=axis)
        return df
            
    def groupby(self, grouper, **args):
        if not isinstance(self._obj, (pd.core.groupby.generic.DataFrameGroupBy, pd.core.groupby.generic.SeriesGroupBy)):
            self._obj = self._obj.groupby(grouper, **args)
        return self

    def getLabelForGroup(self, idx):
      return list(map(lambda x: x[0], self._obj))[idx]
    
    def apply(self, *functions, ignoreGroups=None, onlyGroups=None):
        '''Add function(s) to the pipeline.
           The functions can be limited to certains groups by providing the name of the groups
           to the ignoreGroups or onlyGroups optional arguments.
        '''
        # onlyGroups takes precedence if both only/ignore
        if onlyGroups:
            funcs = [lambda x,f=fn: (x[0], f(x[1])) if x[0] in onlyGroups else (x[0], x[1]) for fn in functions]
        elif ignoreGroups:
            funcs = [lambda x,f=fn: (x[0], f(x[1])) if x[0] not in ignoreGroups else (x[0], x[1]) for fn in functions]
        else:
            # see https://docs.python.org/3/faq/programming.html#why-do-lambdas-defined-in-a-loop-with-different-values-all-return-the-same-result
            funcs = [lambda x,f=fn: (x[0], f(x[1])) for fn in functions]
        self._pipedFunctions.extend(funcs)
        return self
    
    def resetStartingValues(self, ignoreGroups=None, onlyGroups=None):
        self.apply(lambda x: self._execute(x, index=0, operation="subtract"),
                  ignoreGroups=ignoreGroups, onlyGroups=onlyGroups)
        return self

    def subtract(self, index=0, column=None, ignoreGroups=None, onlyGroups=None):
        self.apply(lambda x: self._execute(x, index, column, operation="subtract"),
                  ignoreGroups=ignoreGroups, onlyGroups=onlyGroups)
        return self

    def add(self, index=0, column=None, ignoreGroups=None, onlyGroups=None):
        self.apply(lambda x: self._execute(x, index, column, operation="add"),
                  ignoreGroups=ignoreGroups, onlyGroups=onlyGroups)
        return self

    def multiply(self, index=0, column=None, ignoreGroups=None, onlyGroups=None):
        self.apply(lambda x: self._execute(x, index, column, operation="multiply"),
                  ignoreGroups=ignoreGroups, onlyGroups=onlyGroups)
        return self

    def divide(self, index=0, column=None, ignoreGroups=None, onlyGroups=None):
        self.apply(lambda x: self._execute(x, index, column, operation="divide"),
                  ignoreGroups=ignoreGroups, onlyGroups=onlyGroups)
        return self
    
    def resetIndex(self, handleFuture=True, ignoreGroups=None, onlyGroups=None):
        self.apply(self._resetIndex, ignoreGroups=ignoreGroups, onlyGroups=onlyGroups)
        return self
                            
    def concat(self, multiIndex='hierarchy', sep='|', clearPipeline=True, **kwargs):
        axis = kwargs.pop('axis', 1)
        self._validatePipelineObject(self._obj)
        transformedGroups = self.transformedGroups
        concatenated = pd.concat(map(lambda x: x[1], transformedGroups), axis=axis, **kwargs)
        newNames = self._generateIndexNames(transformedGroups, axis, multiIndex, sep)
        try:
            if axis in [0, 'index']:
                concatenated.index = newNames
            elif axis in [1, 'columns']:
                concatenated.columns = newNames
            else:
                raise ValueError("The axis provided is not of [0 | index | 1 | columns].")
        except:
            pass 
        if clearPipeline:
            self._clearPipeline() # clear piped functions
        return concatenated
    
    def toJSON(self, fileName, rowIndicesFieldName='idx_', addMultiIndexWithSep='|', clearPipeline=True, **kwargs):
        concatenated = self.concat(
            multiIndex="join" if addMultiIndexWithSep else False, 
            sep=addMultiIndexWithSep, 
            clearPipeline=clearPipeline,
            **kwargs)
        dict_toExport = concatenated.to_dict(orient="records")
        if rowIndicesFieldName:
            isIndexTimeType = np.issubdtype(concatenated.index, np.datetime64)
            for i,index in enumerate(concatenated.index):
                idx = index if not isIndexTimeType else index.isoformat()
                dict_toExport[i][rowIndicesFieldName] = f"{idx}"
        with open(fileName, "w") as f:
            f.write(f'{{"data": {json.dumps(dict_toExport)}}}')

    @property
    def pipeline(self):
        return self._pipe(*self._pipedFunctions)

    @property
    def groups(self):
        return list(map(lambda x: (x[0], x[1]), self._obj))

    @property
    def transformedGroups(self):
        return list(map(lambda x: (x[0], self.pipeline(x)[1]), self._obj))
                                    