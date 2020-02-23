# aws-toolbelt
Collection of useful AWS-related tools and utility methods. Refer to [config.js](src/config.js) for how aws-sdk
authentication is handled.

## src/dynamodb.js
Collection of utility methods for DynamoDB. Note: all methods return a promise.

#### `listTable`
Lists all tables. E.g. `dynamodb.listTables()`.

#### `describeTable`
Returns information about the table. E.g. `dynamodb.listTables('user_table')`.

####  `queryTable`
Queries a table by its primary key and optional sort key condition. Examples:
- To query a table named books where the primary key is author: `queryTable('books', 'stephen king')`.
- To add an optional parameter where book title is the sort key: `queryTable('books', 'stephen king', 'the shining')`.
- The optional 3rd parameter can be a key condition object. E.g. if your sort key is a number, you can specify additional operations.
    - If your sort key is creation time and you want to query for all Stephen King books published after 2010: `queryTable('books', 'stephen king', {value: 1262322000000, operation: '>'})`
    - Valid `operation` values: `=`, `>`, `>=`, `<`, `<=`, `between`, `begins_with`.
    - Special case is needed if `between` is used. E.g. if your sort key is creation time and you want to query for all items between 2010 and 2010: `{value1: 946702800000, value2: 1262322000000, operation: 'between'}`

#### `queryTableWithAutoPagination`
Same as `queryTable` but this call will auto-paginate to retrieve all results. DynamoDB limits responses to 1 MB. If it is exceeded, pagination is required. See https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#API_Query_RequestSyntax for additional details.

####  `scanTable`
Scan the specified table with optional filters. Examples
- To scan a table named books: `scanTable('books')`.
- To add an optional parameter where book title contains the word fantasy: `scanTable('books', [{attribute: 'title', value: 'fantasy', operation: 'contains'}])`.
    - Valid `operation` values: `=`, `!=`, `>`, `>=`, `<`, `<=`, `between`, `exists`, `not exists`, `contains`, `not contains`, `begins_with`.
    - Special case is needed if `begins_with` is used. E.g. to filter for all books between 2010 and 2010: `{value1: 946702800000, value2: 1262322000000, operation: 'between'}`

#### `scanTableWithAutoPagination`
Same as `scanTable` but this call will auto-paginate to retrieve all results.
