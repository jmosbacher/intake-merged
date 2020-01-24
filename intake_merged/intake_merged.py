# -*- coding: utf-8 -*-
try:
    import urllib.parse as urlparse
except ImportError:
    import urlparse

from intake.source import base
from . import __version__


class MergedSource(base.DataSource):
    container = 'other'
    version = __version__
    name = 'merged'
    partition_access = False

    def __init__(self, left, right, merge_kwargs, metadata=None):
        """
        Parameters
        ----------
        left: str
            Name of left source to merge, must be a key in the same catalog
        right: str
            Name of right source to merge, must be a key in the same catalog   
        merge_kwargs: dict
            Keyword arguments to pass to merge method.
        metadata: dict or None
            Extra metadata to associate
        kwargs: passed on to targets
        """
        super(MergedSource, self).__init__(metadata)
        self.left = left
        self.right = right
        self.merge_kwargs = merge_kwargs
        self.metadata = metadata
        self.lsource, self.rsource = None, None

    def _get_schema(self):
        l,r = self.lsource.read_partition(0), self.rsource.read_partition(0)
        rows = max(len(l.index)*self.lsource.npartitions, len(r.index)*self.rsource.npartitions) 
        data = self._merge(l,r)
        return base.Schema(datashape=None,
                           dtype=[str(d) for d in data.dtypes],
                           shape=(rows, len(data.columns)),
                           npartitions=1,
                           extra_metadata={})

    def discover(self):
        self._get_source()
        return super().discover()

    def _get_source(self):
        if self.catalog_object is None:
            raise ValueError('MergedDataFrameSource cannot be used outside a catalog')
        if self.lsource is None:
            self.lsource= self.catalog_object[self.left]
        if self.rsource is None:
            self.rsource = self.catalog_object[self.right]
        if self.lsource.container != self.container or self.rsource.container != self.container:
            raise TypeError("Both sources must return a {} to sucessfully merge.".format(self.container))
        self._load_metadata()
        #self.metadata = self.lsource.metadata.copy()
        # self.container = self.lsource.container
        # self.partition_access = self.lsource.partition_access
        # self.description = self.lsource.description
        # self.datashape = self.lsource.datashape

    def _merge_(self, l, r):
        raise NotImplementedError("Subclass must implement this method")
        
    def read(self):
        self._get_source()
        l, r = self.lsource.read(), self.rsource.read()
        return self._merge(l,r)

    def read_partition(self, i):
        return self.read()
        self._get_source()
        # l, r = self.lsource.read_partition(i), self.rsource.read_partition(i)
        # return self._merge(l,r)
    def _close(self):
        pass

class MergedDataFrameSource(MergedSource):
    container = 'dataframe'
    version = __version__
    name = 'merged_df'

    def _merge(self, l, r):
        import pandas as pd
        merged = pd.merge(l, r, **self.merge_kwargs)
        return merged