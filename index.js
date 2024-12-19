function formatPrivateKey(key) {
    if (!key) throw new Error('GOOGLE_PRIVATE_KEY is not set');
    
    // First, clean up the environment variable formatting
    let formattedKey = key.toString()
        .replace('GOOGLE_PRIVATE_KEY Value=', '')  // Remove variable prefix
        .replace(/\\n/g, '\n')  // Handle escaped newlines
        .replace(/["']/g, '')   // Remove any quotes
        .trim();

    // Log the key's format for debugging
    console.log('Private key format check:', {
        hasHeader: formattedKey.includes('-----BEGIN PRIVATE KEY-----'),
        hasFooter: formattedKey.includes('-----END PRIVATE KEY-----'),
        length: formattedKey.length,
        containsNewlines: formattedKey.includes('\n')
    });

    // Ensure proper line breaks for Node 22+
    const rows = formattedKey
        .replace('-----BEGIN PRIVATE KEY-----', '')
        .replace('-----END PRIVATE KEY-----', '')
        .trim()
        .match(/.{1,64}/g) || [];

    // Reconstruct the key with proper formatting
    return `-----BEGIN PRIVATE KEY-----\n${rows.join('\n')}\n-----END PRIVATE KEY-----`;
}

async function getAuth() {
    try {
        console.log('Attempting to authenticate with Google Sheets');
        
        if (!process.env.GOOGLE_CLIENT_EMAIL) {
            throw new Error('GOOGLE_CLIENT_EMAIL is not set');
        }

        const privateKey = formatPrivateKey(process.env.GOOGLE_PRIVATE_KEY);
        
        const auth = new google.auth.GoogleAuth({
            credentials: {
                client_email: process.env.GOOGLE_CLIENT_EMAIL,
                private_key: privateKey
            },
            scopes: ['https://www.googleapis.com/auth/spreadsheets']
        });

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
