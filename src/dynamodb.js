/**
 * Collection of utility methods for DynamoDB. Note: all methods return a promise.
 */

require('./config');

const AWS = require('aws-sdk');

const dynamodb = new AWS.DynamoDB({apiVersion: '2012-08-10'});
const documentClient = new AWS.DynamoDB.DocumentClient(dynamodb);

/**
 * Returns information about the table.
 *
 * @param {string} tableName
 * @returns {Promise}
 */
async function describeTable(tableName) {
  return dynamodb.describeTable({
    TableName: tableName,
  }).promise();
}

/**
 * Lists all tables.
 *
 * @returns {Promise}
 */
async function listTables() {
  return dynamodb.listTables({}).promise();
}

/**
 * Queries a table by its primary key and optional sort key condition.
 *
 * @param {string} tableName
 * @param {string} primaryKeyValue
 * @param {string|Object} [sortKeyValueOrKeyCondition]
 * @returns {Promise}
 */
async function queryTable(tableName, primaryKeyValue, sortKeyValueOrKeyCondition) {
  const {HASH: primaryKey, RANGE: sortKey} = await _getTableKeySchema(tableName);
  const params = _constructQueryParams(tableName, primaryKey, primaryKeyValue, sortKey, sortKeyValueOrKeyCondition);
  return await documentClient.query(params).promise();
}

/**
 * Query the specified table with auto pagination and with optional sort key condition.
 *
 * @param {string} tableName
 * @param {string} primaryKeyValue
 * @param {string|Object} [sortKeyValueOrKeyCondition]
 * @returns {Promise}
 */
async function queryTableWithAutoPagination(tableName, primaryKeyValue, sortKeyValueOrKeyCondition) {
  const {HASH: primaryKey, RANGE: sortKey} = await _getTableKeySchema(tableName);
  const params = _constructQueryParams(tableName, primaryKey, primaryKeyValue, sortKey, sortKeyValueOrKeyCondition);
  return _autoPaginate(params, 'query', {
    Count: 0,
    ScannedCount: 0,
    Items: [],
  });
}

/**
 * Scan the specified table with optional filters.
 *
 * @param {string} tableName
 * @param {Array|Object} [filters]
 * @returns {Promise}
 */
async function scanTable(tableName, filters) {
  const params = _constructScanParams(tableName, filters);
  return await documentClient.scan(params).promise();
}

/**
 * Scan the specified table with auto pagination and with optional filters.
 *
 * @param {string} tableName
 * @param {Array|Object} [filters]
 * @returns {Promise}
 */
async function scanTableWithAutoPagination(tableName, filters) {
  const params = _constructScanParams(tableName, filters);
  return _autoPaginate(params, 'scan', {
    Count: 0,
    ScannedCount: 0,
    Items: [],
  });
}

/**
 * Performs a batch write and automatically chunks requests into 25 items per request if necessary.
 *
 * @param {Array} requests
 * @param {string} tableName
 * @returns {Promise}
 */
async function batchWrite(requests, tableName) {
  // You can only batch write 25 at a time. So lets split it.
  const map = {0: []};
  requests.forEach((request) => {
    const index = Object.keys(map).length - 1;
    if (map[index].length >= 25) {
      const nextIndex = index + 1;
      map[nextIndex] = [];
      map[nextIndex].push(request);
    } else {
      map[index].push(request);
    }
  });

  await _batchWrite(0, map, tableName, true);
}

/**
 * The underlying batchWrite call.
 *
 * @param {number} index
 * @param {Object} map
 * @param {string} tableName
 * @param {boolean} keepGoing
 * @returns {Promise}
 * @private
 */
async function _batchWrite(index, map, tableName, keepGoing) {
  console.warn('Performing batchWrite request ', index + 1, '/', Object.keys(map).length);
  documentClient.batchWrite({
    RequestItems: {
      [tableName]: map[index],
    },
  }, (err, data) => {
    if (err) {
      console.error('err', err);
    }
    if (data && data.UnprocessedItems[tableName]) {
      console.warn('Unprocessed items stop triggered!');
      data.UnprocessedItems[tableName].forEach(console.log);
    } else if (keepGoing) {
      if (index >= Object.keys(map).length - 1) {
        console.log('Done!');
      } else {
        _batchWrite(index + 1, map, tableName, keepGoing);
      }
    }
  });
}

/**
 * Helper method to return the specified table's key schema (hash and range keys).
 * @param {string} tableName
 * @returns {Promise}
 * @private
 */
async function _getTableKeySchema(tableName) {
  const keySchema = (await describeTable(tableName)).Table.KeySchema;
  return keySchema.reduce((map, {AttributeName, KeyType}) => {
    map[KeyType] = AttributeName;
    return map;
  }, {});
}

/**
 * Helper method to construct a params object for table queries.
 *
 * @param {string} tableName
 * @param {string} primaryKey
 * @param {string} primaryKeyValue
 * @param {string} sortKey
 * @param {string|Object} sortKeyValueKeyCondition
 * @returns {Object}
 * @private
 */
function _constructQueryParams(tableName, primaryKey, primaryKeyValue, sortKey, sortKeyValueKeyCondition) {
  const params = {
    TableName: tableName,
    KeyConditionExpression: `${primaryKey} = :pkvalue`,
    ExpressionAttributeValues: {
      ':pkvalue': primaryKeyValue,
    },
  };
  if (sortKeyValueKeyCondition) {
    if (typeof sortKeyValueKeyCondition === 'string') {
      params.KeyConditionExpression += ` and begins_with(${sortKey}, :skvalue)`;
      params.ExpressionAttributeValues[':skvalue'] = sortKeyValueKeyCondition;
    } else {
      const {operation = '=', value, value1, value2} = sortKeyValueKeyCondition;
      const isValidOperation = ['=', '>', '>=', '<', '<=', 'between', 'begins_with'].includes(operation);
      if (!isValidOperation) {
        throw new Error(`Invalid operation "${operation}" used.`);
      }
      if (operation === 'begins_with') {
        params.KeyConditionExpression += ` and begins_with(${sortKey}, :skvalue)`;
        params.ExpressionAttributeValues[':skvalue'] = value;
      } else if (operation === 'between') {
        params.KeyConditionExpression += ` and ${sortKey} between :skvalue1 and :skvalue2 `;
        params.ExpressionAttributeValues[':skvalue1'] = value1;
        params.ExpressionAttributeValues[':skvalue2'] = value2;
      } else {
        params.KeyConditionExpression += ` and ${sortKey} ${operation} :skvalue`;
        params.ExpressionAttributeValues[':skvalue'] = value;
      }
    }
  }
  return params;
}

/**
 * Helper method to construct a params object for table scans.
 *
 * @param {string} tableName
 * @param {Array|Object} filters
 * @returns {Object}
 * @private
 */
function _constructScanParams(tableName, filters) {
  const params = {
    TableName: tableName,
  };
  if (filters) {
    // If multiple filters, iterate.
    if (Array.isArray(filters)) {
      filters.forEach((filter, i) => {
        const {expression, expressionAttributeValues} = _buildFilterQuery(filter);
        if (i > 0) {
          params.FilterExpression += ` and ${expression}`;
          params.ExpressionAttributeValues = {
            ...params.ExpressionAttributeValues,
            ...expressionAttributeValues,
          }
        } else {
          params.FilterExpression = expression;
          params.ExpressionAttributeValues = expressionAttributeValues;
        }
      });
    } else {
      const {expression, expressionAttributeValues} = _buildFilterQuery(filters);
      params.FilterExpression = expression;
      params.ExpressionAttributeValues = expressionAttributeValues;
    }
  }
  return params;
}

/**
 * Helper method to construct a filter query containing a filter expression and expression attribute values.
 *
 * @param {Object} filter
 * @returns {Object}
 * @private
 */
function _buildFilterQuery(filter) {
  const {attribute, operation = '=', value, value1, value2} = filter;
  const isValidOperation = ['=', '!=', '>', '>=', '<', '<=', 'between', 'exists', 'not exists', 'contains', 'not contains', 'begins_with'].includes(operation);
  if (!isValidOperation) {
    throw new Error(`Invalid operation "${operation}" used.`);
  }
  if (operation.includes('begins_with') || operation.includes('contains')) {
    return {
      expression: `${operation}(${attribute}, :${attribute}Value)`,
      expressionAttributeValues: {
        [`:${attribute}Value`]: value,
      },
    }
  }
  if (operation === 'exists') {
    return {
      expression: `attribute_exists(${attribute})`,
      expressionAttributeValues: {},
    }
  }
  if (operation === 'not exists') {
    return {
      expression: `attribute_not_exists(${attribute})`,
      expressionAttributeValues: {},
    }
  }
  if (operation === 'between') {
    return {
      expression: `${attribute} between :${attribute}Value1 and :${attribute}Value2`,
      expressionAttributeValues: {
        [`:${attribute}Value1`]: value1,
        [`:${attribute}Value2`]: value2,
      },
    }
  }
  return {
    expression: `${attribute} ${operation} :${attribute}Value`,
    expressionAttributeValues: {
      [`:${attribute}Value`]: value,
    }
  }
}

/**
 * Recursive auto-pagination method.
 *
 * @param {Object} params
 * @param {string} method
 * @param {Object} aggregatedResults
 * @param {Object} [lastEvaluatedKey]
 * @returns {Promise}
 * @private
 */
async function _autoPaginate(params, method, aggregatedResults, lastEvaluatedKey) {
  if (method !== 'scan' && method !== 'query') {
    throw new Error(`Invalid method ${method} provided.`);
  }
  const queryParams = {
    ...params,
  };
  if (lastEvaluatedKey) {
    console.info(`Starting next ${method} with `, lastEvaluatedKey);
    queryParams.ExclusiveStartKey = lastEvaluatedKey;
  }
  const results = await documentClient[method](queryParams).promise();
  aggregatedResults.Count += results.Count;
  aggregatedResults.ScannedCount += results.ScannedCount;
  aggregatedResults.Items = aggregatedResults.Items.concat(results.Items);
  if (results.LastEvaluatedKey) {
    console.info(`Retrieved ${results.ScannedCount} results. Fetching next set...`);
    return _autoPaginate(params, method, aggregatedResults, results.LastEvaluatedKey);
  }
  return aggregatedResults;
}

module.exports = {
  batchWrite,
  describeTable,
  listTables,
  queryTable,
  queryTableWithAutoPagination,
  scanTable,
  scanTableWithAutoPagination,
};
