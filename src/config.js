/**
 * This script sets the aws-sdk global configuration object with the credentials to use. It will first set the region to
 * from AWS_REGION environment variable, or defaults to us-east-1. Afterwards it will set the credentials if a config
 * JSON file was specified through AWS_CONFIG_FILE. If not provided, it will fall back to checking from your
 * ~/.aws/credentials location or AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables. Refer to
 * https://aws.amazon.com/blogs/security/a-new-and-standardized-way-to-manage-credentials-in-the-aws-sdks/ for
 * additional details.
 */

const AWS = require('aws-sdk');
const fs = require('fs')
const os = require('os');
const path = require('path');

const {AWS_REGION = 'us-east-1', AWS_CONFIG_FILE, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY} = process.env;

console.info(`Initializing AWS DynamoDB with region: ${AWS_REGION}.`);
AWS.config.update({
  region: AWS_REGION,
});

if (AWS_CONFIG_FILE) {
  console.info('Usinging AWS credentials from JSON config file.');
  AWS.config.loadFromPath(path.resolve(AWS_CONFIG_FILE));
} else if (AWS_ACCESS_KEY_ID && AWS_SECRET_ACCESS_KEY) {
  console.info('Using AWS credentials AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.');
} else if (fs.existsSync(path.join(os.homedir(), '.aws', 'credentials'))) {
  console.info('Using AWS credentials from credentials file.');
} else {
  throw new Error('Unable to set AWS credentials.');
}
