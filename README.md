# python-mysql-simple-wrapper

A simple wrapper for operation on mysql.

## Feature

1. Ensure connection.

    It will check the connection before execute any action.
    When disconnected, it will reconnect.

1. Rows can be transformed between dict and list.

1. Batch insert support.

1. Column name can be transported as `*args` or `**kwargs`

## Usage

#### Init db object

```python
In [1]: import mysql_simple_wrapper

In [2]: config = {
   ...:     "host": "hostname",
   ...:     "port": 3060,
   ...:     "user": "test",
   ...:     "password": "test",
   ...:     "database": "test",
   ...: }

In [3]: dbo = mysql_simple_wrapper.Dbmysql(config)
```

#### Fetch and Insert

```python
In [14]: flag, result = dbo.fetch(table='test')

In [15]: print flag, result
True []

In [16]: row = {'value': 'test'}

In [17]: flag, result = dbo.insert(table='test', row=row)

In [18]: print flag, result
True 1

In [19]: dbo.commit()
Out[19]: True

In [20]: flag, result = dbo.fetch(table='test')

In [21]: print result
[{u'kid': 1, u'value': u'test'}]
```

#### Use insertmany

```python
In [22]: rows = [{'value': 'test2'}, {'value': 'test3'}]

In [23]: flag, result = dbo.insertmany(table='test', rows=rows)

In [24]: print flag, result
True 8

In [25]: dbo.fetch(table='test')
Out[25]:
(True,
 [{u'kid': 1, u'value': u'test'},
  {u'kid': 8, u'value': u'test2'},
  {u'kid': 9, u'value': u'test3'}])

In [26]: dbo.commit()  
```

#### Convert the result to dict

```python
In [34]: dbo.fetch(table='test', dict_key='kid')                                                                                                                                                     
Out[34]:
(True,
 {1: {u'value': u'test'}, 8: {u'value': u'test2'}, 9: {u'value': u'test3'}})
```

#### Limit fetched size

```python
In [35]: dbo.fetch(table='test', num=1)
Out[35]: (True, [{u'kid': 1, u'value': u'test'}])
```

#### Specify fetch condition

```python
In [36]: dbo.fetch(table='test', kid=9)
Out[36]: (True, [{u'kid': 9, u'value': u'test3'}])
```
