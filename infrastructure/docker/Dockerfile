FROM node:22-alpine

RUN corepack enable
WORKDIR /app

COPY package.json tsconfig.base.json ./
COPY apps ./apps
COPY packages ./packages

RUN npm install
RUN npm run build

ARG SERVICE=api
ENV SERVICE=${SERVICE}

CMD ["sh", "-lc", "node apps/${SERVICE}/dist/apps/${SERVICE}/src/index.js"]
