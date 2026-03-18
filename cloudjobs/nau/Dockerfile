FROM node:20-bookworm

ENV NODE_ENV=production \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright \
    TZ=Europe/Lisbon

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev
RUN npx playwright install --with-deps chromium

COPY data ./data
COPY scripts ./scripts
COPY README.md ./

RUN mkdir -p output

ENTRYPOINT ["node", "scripts/run-nau-automation.js"]
