const express = require('express');
const { google } = require('googleapis');
const { CloudTasksClient } = require('@google-cloud/tasks');
const { ApifyClient } = require('apify-client');
require('dotenv').config();

const app = express();
app.use(express.json());

// Configuration constants
const APIFY_TOKEN = process.env.APIFY_API_TOKEN;
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const LOG_SHEET_NAME = 'Log';
const POST_METRICS_SHEET_NAME = 'PostMetrics';
const PROGRESS_SHEET_NAME = 'Progress';
const QUOTA_PROJECT_ID = process.env.QUOTA_PROJECT_ID;
const ACTOR_ID = process.env.APIFY_ACTOR_ID;

// Initialize Apify client
const apifyClient = new ApifyClient({
    token: APIFY_TOKEN,
});

// Batch processing constants
const BATCH_SIZE = 10;
const DELAY_MINUTES = 16;
const API_RETRY_DELAY = 60000;
const MAX_RETRIES = 3;

async function getAuth() {
    try {
        const auth = new google.auth.GoogleAuth({
            scopes: ['https://www.googleapis.com/auth/spreadsheets'],
            quota_project_id: QUOTA_PROJECT_ID,
        });
        return await auth.getClient();
    } catch (error) {
        console.error('Authentication error:', error);
        throw new Error('Failed to authenticate with Google Sheets API');
    }
}

function formatDateForSheets(dateStr = new Date()) {
    const date = new Date(dateStr);
    return date.toISOString().replace('T', ' ').slice(0, 19);
}

async function getTweetIdRowIndex(sheetsApi, tweetId) {
    try {
        const response = await sheetsApi.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: `${POST_METRICS_SHEET_NAME}!B2:B`,
        });

        const tweetIds = response.data.values || [];
        const rowIndex = tweetIds.findIndex(row => row[0] === tweetId);
        return rowIndex === -1 ? -1 : rowIndex + 2;
    } catch (error) {
        console.error('Error finding tweet ID:', error);
        throw new Error('Failed to search for tweet ID in spreadsheet');
    }
}

async function getPostMetrics(postId) {
    try {
        // Start the Apify actor run
        const run = await apifyClient.actor(ACTOR_ID).call({
            postUrls: [`https://twitter.com/i/web/status/${postId}`],
            maxRequestRetries: 3,
        });

        // Wait for the run to finish and get the dataset items
        const { items } = await apifyClient.dataset(run.defaultDatasetId).listItems();
        
        if (!items || items.length === 0) {
            throw new Error(`No data found for post ${postId}`);
        }

        return items[0]; // Return the first item since we're only scraping one post
    } catch (error) {
        console.error(`Error fetching metrics for post ${postId}:`, error);
        throw error;
    }
}

async function updateProgress(sheetsApi, { currentBatch, totalBatches, status, lastProcessedId }) {
    try {
        await sheetsApi.spreadsheets.values.update({
            spreadsheetId: SPREADSHEET_ID,
            range: `${PROGRESS_SHEET_NAME}!A1:E1`,
            valueInputOption: 'USER_ENTERED',
            resource: {
                values: [[
                    status,
                    currentBatch,
                    totalBatches,
                    `${Math.round((currentBatch / totalBatches) * 100)}%`,
                    lastProcessedId || 'N/A'
                ]]
            }
        });
    } catch (error) {
        console.error('Error updating progress:', error);
    }
}

async function scheduleNextBatch(nextStartIndex) {
    console.log(`Scheduling next batch starting at index ${nextStartIndex}`);
    const tasksClient = new CloudTasksClient();
    const project = QUOTA_PROJECT_ID;
    const queue = 'tweet-metrics-queue';
    const location = 'us-central1';
    const url = 'https://apify-metrics-service-url/processTweetMetricsBatch'; // Update with your service URL

    const task = {
        httpRequest: {
            httpMethod: 'POST',
            url,
            oidcToken: {
                serviceAccountEmail: `gcf-sheets-service-account@${QUOTA_PROJECT_ID}.iam.gserviceaccount.com`,
            },
            body: Buffer.from(JSON.stringify({ startIndex: nextStartIndex })).toString('base64'),
            headers: {
                'Content-Type': 'application/json',
            },
        },
        scheduleTime: {
            seconds: Math.floor(Date.now() / 1000) + DELAY_MINUTES * 60,
        },
    };

    try {
        const [response] = await tasksClient.createTask({
            parent: tasksClient.queuePath(project, location, queue),
            task,
        });
        console.log(`Successfully scheduled next batch. Task name: ${response.name}`);
    } catch (error) {
        console.error('Error scheduling next batch:', error);
        throw error;
    }
}

app.post('/processTweetMetricsBatch', async (req, res) => {
    // [Batch processing code - continued in next update]
});

app.post('/processTweetMetrics', async (req, res) => {
    // [Single tweet processing code - continued in next update]
});

app.get('/', (req, res) => {
    res.send('Welcome! This is the Apify Metrics Service.');
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
});