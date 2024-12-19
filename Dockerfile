# Use an official Node.js runtime as a parent image
FROM node:16.20.0-alpine
# Set working directory
WORKDIR /app
# Copy package.json and package-lock.json
COPY package*.json ./
# Install dependencies
RUN npm install
# Copy the rest of the application code
COPY . .
# Expose the port the app runs on
EXPOSE 8080
# Define the command to run the application
CMD ["node", "--openssl-legacy-provider", "index.js"]
