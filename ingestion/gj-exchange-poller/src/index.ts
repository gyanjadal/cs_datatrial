import * as AWS from 'aws-sdk';
import axios from 'axios';
import { format } from 'date-fns';

const s3 = new AWS.S3();

/*
* Description: Handler for invoking exchange api and storing in S3 bucket
* Params: 
* event: Json string (schema in example below)
*/
export const handler = async (event: any) => {
    // Extract parameters from the event object
    const runDateTime = new Date();
    const { url, bucketName, exchangeName, productName, prefix } = event;
    let response = null;
    try {
        // Polling the URL
        console.log(`Polling ${url}`);
        response = await axios.get(url);
        console.log(`Successfully polled ${url}. Response status is ${response.status}. Response length is ${response.data.toString().length}`);
    }
    catch (error) {
        console.error(`Error polling ${url}. Response status is ${response?.status}:  ${error}`);
        throw error;
    }

    let fileName = null;
    try {
        // Storing the response in S3
        fileName = getFileName(exchangeName, productName, prefix, runDateTime);
        console.log(`Storing response from ${url} to S3 bucket ${bucketName}/$${fileName}`);
        await s3.putObject({
            Bucket: bucketName,
            Key: fileName,
            Body: JSON.stringify(response?.data),
            ContentType: 'application/json'
        }).promise();
        console.log(`Successfully stored response from ${url} to S3 bucket ${bucketName}/${fileName}`);
    }
    catch (error) {
        console.error(`Error storing response from ${url} and storing to S3 bucket ${bucketName}/${fileName}: ${error}`);
        throw error;
    }
};

/*
* Description: Construct folder and filepath for the ingested response
* Params: subBucketName based on the currency/stock, 
*         runDateTime of the current run of the function, 
*         prefix of the file created
*/
function getFileName(exchangeName: string, productName: string, prefix: string, runDateTime: Date) {
    return `${exchangeName}/${productName}/${format(runDateTime, 'yyyyMMdd')}/${format(runDateTime, 'HH')}/${format(runDateTime, 'mmss')}_${productName.toLowerCase()}_${prefix}.json:`;
}

// Example usage:
const event = {
    "url": "https://api.pro.coinbase.com/products/ETH-USD/book?level=2",
    "bucketName": "gj-ingestion-bucket",
    "exchangeName": "Coinbase",
    "productName": "ETH-USD",
    "prefix": "book_snapshot"
  }
handler(event);