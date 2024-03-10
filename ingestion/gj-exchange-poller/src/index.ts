import * as AWS from 'aws-sdk';
import axios from 'axios';
import { format } from 'date-fns';

const s3 = new AWS.S3();

export const handler = async (event: any) => {
    // Extract parameters from the event object
    const { url, bucketName, subBucketName, prefix } = event;
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
        fileName = getFileName(subBucketName, prefix);
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

function getFileName(subBucketName: string, prefix: string) {
    return `${subBucketName}/${formatCurrentDateTime()}_${subBucketName.toLowerCase()}_${prefix}.json:`;
}

function formatCurrentDateTime(): string {
    const currentDate = new Date();
    return format(currentDate, 'yyyyMMdd_HHmmss');
}

// Example usage:
const event = {
    "url": "https://api.pro.coinbase.com/products/ETH-USD/book?level=2",
    "bucketName": "gj-ingestion-bucket",
    "subBucketName": "ETH-USD",
    "prefix": "book_snapshot"
  }

handler(event);