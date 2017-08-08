FROM node:alpine

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY package.json .
RUN npm install && npm cache clean --force
COPY . .

CMD ["node", "worker.js"]
