import happybase

connection = happybase.Connection('hostname')
connection.create_table(
    'mytable',
    {
     'cf1': dict(),  # use defaults
    }
)
table = connection.table('mytable')

table.put(b'row-key', {b'cf1:name': b'ben',
                       b'cf1:age': b'18'})

for key, data in table.scan(row_prefix=b'row'):
    print(key, data)  # prints 'value1' and 'value2'