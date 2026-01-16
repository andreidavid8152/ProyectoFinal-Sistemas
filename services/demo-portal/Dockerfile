FROM node:20-alpine

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev

COPY public ./public
COPY src ./src

CMD ["npm", "start"]
