FROM node:18.19-alpine

WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .

ENV NODE_OPTIONS="--openssl-legacy-provider"
EXPOSE 8080

ENTRYPOINT ["node"]
CMD ["index.js"]
