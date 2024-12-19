const express = require('express');
const { google } = require('googleapis');
const axios = require('axios');
const crypto = require('crypto');
require('dotenv').config();

const app = express();
app.use(express.json());

// Configuration constants
const APIFY_API_TOKEN = process.env.APIFY_API_TOKEN;
const SPREADSHEET_ID = process.env.SPREADSHEET_ID;
const LOG_SHEET_NAME = 'Log';
const POST_METRICS_SHEET_NAME = 'PostMetrics';

// Add environment variable logging
console.log('Environment variables check:', {
    hasApifyToken: !!process.env.APIFY_API_TOKEN,
    hasSpreadsheetId: !!process.env.SPREADSHEET_ID,
    hasGoogleEmail: !!process.env.GOOGLE_CLIENT_EMAIL,
    hasGoogleKey: !!process.env.GOOGLE_PRIVATE_KEY
});

function formatPrivateKey(key) {
    if (!key) throw new Error('GOOGLE_PRIVATE_KEY is not set');
    
    console.log('Starting key formatting...');
    
    try {
        // Remove the environment variable prefix if it exists
        let rawKey = key.toString().replace('GOOGLE_PRIVATE_KEY Value=', '').trim();
        
        // Convert escaped newlines to actual newlines
        rawKey = rawKey.replace(/\\n/g, '\n');
        
        // Remove any quotes that might be present
        rawKey = rawKey.replace(/^["']|["']$/g, '');
        
        // Log key properties for debugging
        console.log('Key properties:', {
            hasCorrectStart: rawKey.includes('-----BEGIN PRIVATE KEY-----'),
            hasCorrectEnd: rawKey.includes('-----END PRIVATE KEY-----'),
            containsNewlines: rawKey.includes('\n'),
            length: rawKey.length
        });

        return rawKey;
    } catch (error) {
        console.error('Error formatting private key:', error);
        throw error;
    }
}

async function getAuth() {
    try {
        console.log('Starting Google Sheets authentication process...');
        
        if (!process.env.GOOGLE_CLIENT_EMAIL) {
            throw new Error('GOOGLE_CLIENT_EMAIL is not set');
        }

        console.log('Formatting private key...');
        const privateKey = formatPrivateKey(process.env.GOOGLE_PRIVATE_KEY);
        
        // Force Node to use legacy OpenSSL provider
        if (crypto.setProvider) {
            crypto.setProvider('legacy');
        }
        
        console.log('Creating auth client...');
        const auth = new google.auth.GoogleAuth({
            credentials: {
                client_email: process.env.GOOGLE_CLIENT_EMAIL,
                private_key: privateKey
            },
            scopes: ['https://www.googleapis.com/auth/spreadsheets']
        });

        console.log('Getting client...');
        const client = await auth.getClient();
        console.log('Authentication successful');
        return client;
    } catch (error) {
        console.error('Authentication Error:', {
            message: error.message,
            code: error.code,
            stack: error.stack
        });
        throw error;
    }
}

async function fetchTweetMetrics(tweetIds) {
    try {
        console.log('Fetching tweet metrics for', tweetIds.length, 'tweets');
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

        console.log('Successfully fetched tweet metrics');
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

        console.log('Fetching tweet IDs from log sheet...');
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

        console.log('Processing tweet metrics...');
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
        console.log('Received processTweetMetrics request');
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
    console.log(`Server starting on port ${PORT}`);
    console.log('Initializing service...');
    // Trigger initial processing when server starts
    processTweetMetrics().catch(err => {
        console.error('Error during initial processing:', err);
    });
});

module.exports = app;
