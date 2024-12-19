const express = require('express');
const { google } = require('googleapis');
const axios = require('axios');
require('dotenv').config();

const app = express();
app.use(express.json());

// Configuration constants
const APIFY_API_TOKEN = process.env.APIFY_API_TOKEN;
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const LOG_SHEET_NAME = 'Log';
const POST_METRICS_SHEET_NAME = 'PostMetrics';

function formatPrivateKey(key) {
    if (!key) throw new Error('GOOGLE_PRIVATE_KEY is not set');
    
    // Replace escaped newlines
    let formattedKey = key.replace(/\\n/g, '\n');
    
    // Ensure key has proper PEM format
    if (!formattedKey.startsWith('-----BEGIN PRIVATE KEY-----')) {
        formattedKey = `-----BEGIN PRIVATE KEY-----\n${formattedKey}`;
    }
    if (!formattedKey.endsWith('-----END PRIVATE KEY-----')) {
        formattedKey = `${formattedKey}\n-----END PRIVATE KEY-----`;
    }
    
    return formattedKey;
}

async function getAuth() {
    try {
        console.log('Attempting to authenticate with Google Sheets');
        
        // Log environment variables for debugging
        console.log('GOOGLE_CLIENT_EMAIL:', process.env.GOOGLE_CLIENT_EMAIL);
        console.log('GOOGLE_PRIVATE_KEY exists:', !!process.env.GOOGLE_PRIVATE_KEY);

        // Validate environment variables
        if (!process.env.GOOGLE_CLIENT_EMAIL) {
            throw new Error('GOOGLE_CLIENT_EMAIL is not set');
        }

        const credentials = {
            client_email: process.env.GOOGLE_CLIENT_EMAIL,
            private_key: formatPrivateKey(process.env.GOOGLE_PRIVATE_KEY)
        };

        const auth = new google.auth.GoogleAuth({
            credentials: credentials,
            scopes: ['https://www.googleapis.com/auth/spreadsheets']
        });

        const client = await auth.getClient();
        console.log('Authentication successful');
        return client;
    } catch (error) {
        console.error('Full Authentication Error:', {
            message: error.message,
            stack: error.stack,
            env: {
                CLIENT_EMAIL: process.env.GOOGLE_CLIENT_EMAIL,
                PRIVATE_KEY_EXISTS: !!process.env.GOOGLE_PRIVATE_KEY
            }
        });
        throw error;
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

async function processTweetMetrics() {
    try {
        console.log('Starting tweet metrics processing');
        const authClient = await getAuth();
        const sheetsApi = google.sheets({ version: 'v4', auth: authClient });

        // Fetch all tweet IDs from log sheet
        const logResponse = await sheetsApi.spreadsheets.values.get({
            spreadsheetId: SPREADSHEET_ID,
            range: `${LOG_SHEET_NAME}!A2:D`,
        });

        if (!logResponse.data.values) {
            throw new Error('No data found in Log sheet');
        }

        const logData = logResponse.data.values;
        const tweetIds = logData.map(row => row[3]).filter(Boolean);
        console.log(`Found ${tweetIds.length} tweets to process`);

        // Fetch metrics for all tweets at once
        const metricsData = await fetchTweetMetrics(tweetIds);
        console.log(`Fetched metrics for ${metricsData.length} tweets`);

        // Process each tweet's metrics in parallel
        const updatePromises = metricsData.map(tweetData => {
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

            return sheetsApi.spreadsheets.values.append({
                spreadsheetId: SPREADSHEET_ID,
                range: `${POST_METRICS_SHEET_NAME}!A1:L`,
                valueInputOption: 'USER_ENTERED',
                resource: { values: [rowData] }
            });
        });

        await Promise.all(updatePromises);
        console.log('Successfully processed all tweets');
    } catch (error) {
        console.error('Error processing tweets:', error);
        throw error;
    }
}

// Endpoint to trigger tweet metrics processing
app.post('/processTweetMetrics', async (req, res) => {
    try {
        await processTweetMetrics();
        res.status(200).send('Tweet metrics processing completed');
    } catch (error) {
        console.error('Error processing tweets:', error);
        res.status(500).send(`Error: ${error.message}`);
    }
});

app.get('/', (req, res) => {
    res.send('Welcome! This is the Apify Metrics Service.');
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
    // Trigger initial processing when server starts
    processTweetMetrics();
});

module.exports = app;