const formatGooglePrivateKey = (key) => {
  if (!key) throw new Error('Google private key is required');
  
  // Replace escaped newlines with actual newlines
  let formattedKey = key.replace(/\\n/g, '\n');
  
  // Ensure key has proper PEM format headers if they're missing
  if (!formattedKey.includes('-----BEGIN PRIVATE KEY-----')) {
    formattedKey = `-----BEGIN PRIVATE KEY-----\n${formattedKey}`;
  }
  if (!formattedKey.includes('-----END PRIVATE KEY-----')) {
    formattedKey = `${formattedKey}\n-----END PRIVATE KEY-----`;
  }
  
  return formattedKey;
};

module.exports = { formatGooglePrivateKey };