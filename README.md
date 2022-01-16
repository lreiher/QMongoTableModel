# QMongoTableModel

Specialization of PyQt's `QAbstractTableModel` for accessing [MongoDB](https://www.mongodb.com) databases and displaying database contents in `QTableView` or similar.

Two model classes are available:
- `QBaseMongoTableModel` provides a data model for MongoDB queries
- `QMongoTableModel` providess a sortable data model for MongoDB queries

The model can be filled by querying documents via `model.setQuery(db, collection, query)`. Documents are fetched from the database on demand and cached.
