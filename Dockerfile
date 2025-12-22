FROM node:lts-alpine

ARG VERSION latest

RUN npm install -g @shm-open/code-push-server@${VERSION} pm2@latest --no-optional

RUN mkdir /data/

WORKDIR /data/

COPY ./process.json /data/process.json

 # CMD ["pm2-runtime", "/data/process.json"]
 # workaround for issue https://github.com/Unitech/pm2/issues/4950
 CMD ["sh", "-c", "pm2 ps && pm2-runtime /data/process.json"]
