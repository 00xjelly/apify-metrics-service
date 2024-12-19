const express = require('express');
const { google } = require('googleapis');
const axios = require('axios');
const { CloudTasksClient } = require('@google-cloud/tasks');
require('dotenv').config();

const app = express();
app.use(express.json());

// Configuration constants
const APIFY_API_TOKEN = process.env.APIFY_API_TOKEN;
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const LOG_SHEET_NAME = 'Log';
const POST_METRICS_SHEET_NAME = 'PostMetrics';
const PROGRESS_SHEET_NAME = 'Progress';
const QUOTA_PROJECT_ID = process.env.QUOTA_PROJECT_ID;

// Constants for batch processing
const BATCH_SIZE = 10;
const DELAY_MINUTES = 16;

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

async function fetchTweetMetrics(tweetIds) {
    try {
        const response = await axios.post(
            'https://api.apify.com/v2/acts/kaitoeasyapi~twitter-x-data-tweet-scraper-pay-per-result-cheapest/run-sync-get-dataset-items',
            {
                tweetIDs: tweetIds,
                maxItems: tweetIds.length,
                queryType: "Latest"
            },
            {
                headers: {
                    'Authorization': `Bearer ${APIFY_API_TOKEN}`,
                    'Content-Type': 'application/json'
                }
            }
        );

        return response.data;
    } catch (error) {
        console.error('Error fetching tweet metrics:', error);
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
    const url = process.env.SERVICE_URL + '/processTweetMetricsBatch';

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
    console.log('Received request to process batch:', req.body);
    try {
        const { startIndex = 0 } = req.body || {};
        const authClient = await getAuth();
        const sheetsApi = google.sheets({ version: 'v4', auth: authClient });

        // Fetch tweet IDs from log sheet
        const logResponse = await sheetsApi.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: `${LOG_SHEET_NAME}!A2:D`,
        });

        if (!logResponse.data.values) {
            throw new Error('No data found in Log sheet');
        }

        const logData = logResponse.data.values;
        const totalBatches = Math.ceil(logData.length / BATCH_SIZE);
        const currentBatch = Math.floor(startIndex / BATCH_SIZE) + 1;
        const batchEndIndex = Math.min(startIndex + BATCH_SIZE, logData.length);

        // Get tweet IDs for this batch
        const tweetIds = [];
        for (let i = startIndex; i < batchEndIndex; i++) {
            const [, , , tweetId] = logData[i];
            if (tweetId) tweetIds.push(tweetId);
        }

        await updateProgress(sheetsApi, {
            status: 'PROCESSING',
            currentBatch,
            totalBatches,
            lastProcessedId: 'Starting batch...'
        });

        // Fetch metrics for all tweets in batch
        const metricsData = await fetchTweetMetrics(tweetIds);

        // Process each tweet's metrics
        for (const tweetData of metricsData) {
            const rowData = [
                tweetData.created_at,
                tweetData.id,
                tweetData.url,
                tweetData.created_at.split('T')[0],
                tweetData.public_metrics?.impression_count || 0,
                tweetData.public_metrics?.like_count || 0,
                tweetData.public_metrics?.reply_count || 0,
                tweetData.public_metrics?.retweet_count || 0,
                'N/A',
                new Date().toISOString().replace('T', ' ').slice(0, 19),
                tweetData.url,
                tweetData.text || 'N/A'
            ];

            // Update or append to sheet
            try {
                await sheetsApi.spreadsheets.values.append({
                    spreadsheetId: SPREADSHEET_ID,
                    range: `${POST_METRICS_SHEET_NAME}!A1:L`,
                    valueInputOption: 'USER_ENTERED',
                    resource: { values: [rowData] }
                });
            } catch (error) {
                console.error(`Error updating sheet for tweet ${tweetData.id}:`, error);
            }
        }

        // Schedule next batch if needed
        if (batchEndIndex < logData.length) {
            await scheduleNextBatch(batchEndIndex);
            await updateProgress(sheetsApi, {
                status: 'SCHEDULED_NEXT_BATCH',
                currentBatch,
                totalBatches,
                lastProcessedId: tweetIds[tweetIds.length - 1]
            });
        } else {
            await updateProgress(sheetsApi, {
                status: 'COMPLETED',
                currentBatch: totalBatches,
                totalBatches,
                lastProcessedId: tweetIds[tweetIds.length - 1]
            });
        }

        res.status(200).send(`Processed batch ${currentBatch} of ${totalBatches}`);
    } catch (error) {
        console.error('Error processing batch:', error);
        res.status(500).send(`Error processing batch: ${error.message}`);
    }
});

app.get('/', (req, res) => {
    res.send('Welcome! This is the Apify Metrics Service.');
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
});