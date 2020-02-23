const dynamodb = require('./src/dynamodb');

(async () => {
  try {
    console.info('AWS Toolkit.');
    console.info ('Starting some sample calls...');
    console.info('Listing your tables...');
    const tables = await dynamodb.listTables();
    console.info(tables);

    console.info('Describing each table...');
    tables.TableNames.forEach((table) => {
      dynamodb.describeTable(table).then((result) => {
        console.info(table, ': ', result);
      });
    });
  } catch (err) {
    console.error(err);
  }
})();
