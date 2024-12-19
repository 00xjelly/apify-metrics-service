FROM node:18.19-alpine

WORKDIR /app

COPY package*.json ./
COPY index.js ./

RUN npm install

CMD ["npm", "start"]
