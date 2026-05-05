FROM node:20-slim AS builder
RUN apt-get update && apt-get install -y python3 make g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY tsconfig.json ./
COPY src/ src/
RUN npm run build

FROM node:20-slim
RUN apt-get update && apt-get install -y python3 make g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev && npm cache clean --force && apt-get purge -y python3 make g++ && apt-get autoremove -y
COPY --from=builder /app/dist dist/
COPY data/ data/
RUN chown -R node:node /app/data
USER node
ENV NODE_ENV=production
ENV PORT=3000
EXPOSE 3000
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD node -e "require('http').get('http://localhost:' + process.env.PORT + '/health', (r) => { process.exit(r.statusCode === 200 ? 0 : 1) })"
CMD ["node", "dist/http-server.js"]
