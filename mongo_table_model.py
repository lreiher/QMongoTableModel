import functools
from datetime import datetime, timezone

import bson.json_util as bson_util
from bson import ObjectId
from pymongo.errors import PyMongoError
from PyQt5.QtCore import (
    QAbstractTableModel,
    QModelIndex,
    QSortFilterProxyModel,
    Qt,
    QVariant,
)


def instance_lru_cache(maxsize=128, typed=False):
    """functools.lru_cache equivalent for class instances.

    Inspired by https://stackoverflow.com/a/39295217/7264974
    """

    def lru_cache_decorator(func):

        @functools.wraps(func)
        def lru_cache_factory(self, *args, **kwargs):

            # get instance cache size, if defined
            if hasattr(self, "_instance_lru_cache_size"):
                maxsize = self._instance_lru_cache_size

            # wrap the function in a cache by calling the lru_cache decorator
            cache = functools.lru_cache(maxsize, typed)(func)

            # bind the decorated function to the instance to make it a method
            cache = cache.__get__(self, self.__class__)
            setattr(self, func.__name__, cache)

            # call instance cache; next method call will directly go to cache
            return cache(*args, **kwargs)

        return lru_cache_factory

    return lru_cache_decorator


class BaseMongoTableModel(QAbstractTableModel):
    """The BaseMongoTableModel provides a data model for MongoDB queries.

    The BaseMongoTableModel can be used to provide data obtained from MongoDB
    queries to view classes such as QTableView. The derived MongoTableModel
    additionally supports sorting.

    Documents are fetched from the database on demand and cached.

    Attributes:
        header: sorted (nested) field names of queried documents
        cursor: pymongo cursor pointing to the result set of the query
        n_docs: number of documents returned by query
        max_nesting: maximum nesting level to provide columns for
        bson_options: `bson.json_util.JSONOptions` formatting options
    """

    def __init__(self,
                 parent=None,
                 max_nesting=0,
                 cache_size=50,
                 bson_options=bson_util.DEFAULT_JSON_OPTIONS):
        """Creates an empty BaseMongoTableModel.

        Args:
            parent: Qt parent (None)
            max_nesting: maximum nesting level to provide columns for (0)
            cache_size: document cache size (50)
            bson_options: `bson.json_util.JSONOptions` formatting options
        """

        super(BaseMongoTableModel, self).__init__(parent)

        # attributes
        self.max_nesting = max_nesting
        self.bson_options = bson_options
        self.header = []
        self.cursor = None
        self.n_docs = 0

        # set instance cache size
        self._instance_lru_cache_size = cache_size

    def rowCount(self, parent=QModelIndex()):
        """Returns the number of rows, i.e. number of documents.

        Overrides `QAbstractTableModel.rowCount`.

        Args:
            parent: parent model index (`QModelIndex()`)

        Returns:
            number of rows
        """

        if parent.isValid():
            return 0

        return self.n_docs

    def columnCount(self, parent=QModelIndex()):
        """Returns the number of columns, i.e. number of (nested) fields.

        Overrides `QAbstractTableModel.columnCount`.

        Args:
            parent: parent model index (`QModelIndex()`)

        Returns:
            number of columns
        """

        return len(self.header)

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        """Returns the header data for the specified section.

        Returns the (nested) field names of the queried documents for horizontal
        headers. Returns the row number for vertical headers.

        Overrides `QAbstractTableModel.headerData`.

        Args:
            section: section index
            orientation: header orientation
            role: data role (`Qt.DisplayRole`)

        Returns:
            header data
        """

        if role != Qt.DisplayRole:
            return QVariant()

        if orientation == Qt.Horizontal:  # (nested) field names
            try:
                return self.header[section]
            except IndexError:
                return QVariant()
        elif orientation == Qt.Vertical:  # row number
            return section + 1
        else:
            return QVariant()

    def data(self, index, role=Qt.DisplayRole):
        """Returns the data referred to by the model index.

        Returns the field value of the field referred to by the index column of
        the document referred to by the index row. Returns different data
        representations based on the specified role.

        Overrides `QAbstractTableModel.data`.

        Args:
            index: model index
            role: data role (`Qt.DisplayRole`)

        Returns:
            model data
        """

        if not index.isValid():
            return QVariant()

        # get document by row
        doc = self.documentAtIndex(index.row())
        if doc is None:
            return QVariant()

        # update header
        self.updateHeader(doc)

        # get (nested) field value by header key
        try:
            key = self.header[index.column()]
            keys = key.split(".")
            value = doc
            for key in keys:
                value = value[key]
        except (IndexError, KeyError):
            return QVariant()

        # determine best string representation
        if role == Qt.DisplayRole:  # standard table view -> standard string
            if type(value) == ObjectId:
                data = str(value)
            elif type(value) == datetime:
                # return as local time in ISO8601
                data = value.replace(
                    tzinfo=timezone.utc).astimezone().isoformat()
            else:
                data = str(value)
        elif role == Qt.ToolTipRole:  # tooltip view -> formatted json
            data = bson_util.dumps(value,
                                   indent=2,
                                   json_options=self.bson_options)
        elif role == Qt.UserRole:  # user role -> underlying object
            data = value
        else:
            data = QVariant()

        return data

    @instance_lru_cache()
    def documentAtIndex(self, index):
        """Fetches the document at the given cursor index from database/cache.

        Args:
            index: cursor index of document

        Returns:
            document at given cursor index
        """

        try:
            doc = self.cursor[index]

        except IndexError:  # handle document deletion

            # If cursor intially points to documents [d0, d1, d2] and then
            # d1 is externally deleted, the cursor automatically points to
            # [d0, d2], i.e. cursor[1] = d2.
            # cursor[2] will however raise an IndexError, which is handled here
            # by resetting self.n_docs.
            n_docs = self.cursor.count()
            n_docs_delta = n_docs - self.n_docs
            if n_docs_delta < 0:
                self.beginRemoveRows(QModelIndex(), self.n_docs + n_docs_delta,
                                     self.n_docs - 1)
                self.n_docs = n_docs
                self.endRemoveRows()
            doc = None

            # invalidate cache
            self.documentAtIndex.cache_clear()

        except PyMongoError:  # handle other pymongo errors, e.g. disconnect

            doc = None

        return doc

    def documentIdAtIndex(self, index):
        """Returns the ID of the document at the given cursor index.

        Args:
            index: cursor index of document

        Returns:
            document ID at given cursor index
        """

        return str(self.value(index, "_id"))

    def value(self, row, field, role=Qt.UserRole):
        """Returns the field value of the document selected by the row.

        Returns the underlying object if no particular role is specified.
        Returns `None`, if field is not in document.

        Args:
            row: model index row
            field: (nested) field name
            role: data role (`Qt.UserRole`)

        Returns:
            model data
        """

        if field in self.header:
            column = self.header.index(field)
            index = self.index(row, column)
            value = self.data(index, role)
        else:
            value = None

        return value

    def empty(self):
        """Returns whether the model is empty.

        Returns:
            whether the model is empty
        """

        return self.rowCount() == 0

    def updateHeader(self, doc):
        """Updates the header to contain all document keys up to max. nesting.

        All previous header elements remain. If the given document contains new
        keys, the header is extended and new columns are created.    

        Args:
            doc: document
        """

        # define recursive helper function to get all keys up to certain depth
        def getKeys(doc, depth=0, root=None):
            keys = []
            for key, value in doc.items():
                nested_key = f"{root}.{key}" if root is not None else key
                if depth > 0 and type(value) is dict:
                    nested_keys = getKeys(value, depth - 1, nested_key)
                    keys.extend(nested_keys)
                else:
                    keys.append(nested_key)
            return keys

        # determine new header keys
        doc_keys = getKeys(doc, self.max_nesting)
        new_keys = set(doc_keys).difference(self.header)

        # update header with new keys
        for key in new_keys:
            header = sorted(self.header + [key])  # alphabetical sort
            header = list(dict.fromkeys(header))  # guarantee uniqueness
            section = header.index(key)
            self.beginInsertColumns(QModelIndex(), section, section)
            self.header = header
            self.endInsertColumns()

    def setQuery(self, db, collection, query):
        """Resets the model to provide the data resulting from the given query.

        Args:
            db: pymongo database instance
            collection: collection
            query: query dict
        """

        self.beginResetModel()

        # setup query cursor
        self.cursor = db[collection].find(query,
                                          no_cursor_timeout=True).sort("_id")
        self.n_docs = db[collection].count_documents(query)

        # invalidate cache
        try:
            self.documentAtIndex.cache_clear()
        except AttributeError:  # in case the function hasn't been decorated yet
            pass

        # reset header
        self.header = ["_id"]

        self.endResetModel()


class MongoTableModel(QSortFilterProxyModel):
    """The MongoTableModel provides a sortable data model for MongoDB queries.

    The MongoTableModel can be used to provide data obtained from MongoDB
    queries to view classes such as QTableView.
    """

    def __init__(self,
                 parent=None,
                 max_nesting=0,
                 cache_size=50,
                 bson_options=bson_util.DEFAULT_JSON_OPTIONS):
        """Creates an empty MongoTableModel.

        Args:
            parent: Qt parent (None)
            max_nesting: maximum nesting level to provide columns for
            cache_size: document cache size (50)
            bson_options: `bson.json_util.JSONOptions` formatting options
        """

        super(MongoTableModel, self).__init__(parent)

        # set proxy source model
        source_model = BaseMongoTableModel(self, max_nesting, cache_size,
                                           bson_options)
        self.setSourceModel(source_model)

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        """Returns the header data for the specified section.

        Overrides `QSortFilterProxyModel.headerData` to not change the row
        numbers when sorting.

        Args:
            section: section index
            orientation: header orientation
            role: data role (`Qt.DisplayRole`)

        Returns:
            header data
        """

        # call source model implementation irrelevant of current sorting order
        return self.sourceModel().headerData(section, orientation, role)

    def __getattr__(self, attr):

        # forward unknown variable/function invocations to source model
        return getattr(self.sourceModel(), attr)
