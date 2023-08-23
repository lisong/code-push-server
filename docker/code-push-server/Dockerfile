FROM node:8.11.4-alpine

RUN npm config set registry https://registry.npmmirror.com/ \
&& npm i -g code-push-server@0.5.2 pm2@latest --no-optional

COPY ./process.json /process.json

EXPOSE 3000

CMD ["pm2-docker", "start", "/process.json"]
