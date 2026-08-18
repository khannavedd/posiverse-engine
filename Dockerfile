# node:20-slim instead of the full node:20 image — same Node version,
# much smaller. No build/compile step in this app (plain JS), so a
# multi-stage build wouldn't buy anything beyond what --omit=dev
# already does below.
FROM node:20-slim

WORKDIR /usr/src/app

# package*.json copied and installed before the rest of the source so
# this layer only re-runs when dependencies actually change.
COPY package*.json ./

# npm ci (not install) — installs exactly what package-lock.json
# specifies. --omit=dev drops nodemon (the only devDependency).
RUN npm ci --omit=dev

# .dockerignore keeps node_modules/.env out of the build context.
COPY . .

# Run as the image's built-in unprivileged "node" user instead of root.
USER node

EXPOSE 8081

CMD ["npm", "start"]
