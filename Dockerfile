FROM debian

MAINTAINER Andy Halper <ashalper@usgs.gov>

RUN groupadd -r nodejs && useradd -d /home/nodejs -r -g nodejs nodejs

RUN apt-get update -y && apt-get install --no-install-recommends -y -q curl python build-essential git ca-certificates unzip

RUN mkdir /nodejs && curl http://nodejs.org/dist/v0.10.45/node-v0.10.45-linux-x64.tar.gz | tar xvzf - -C /nodejs --strip-components=1

ENV PATH $PATH:/nodejs/bin

RUN mkdir -p /home/nodejs/aq2rdb

RUN chown -R nodejs:nodejs /home/nodejs/

USER nodejs

WORKDIR /home/nodejs

RUN curl -o /home/nodejs/aq2rdb/aq2rdb.zip https://codeload.github.com/ashalper-usgs/aq2rdb/zip/1.3.x

WORKDIR /home/nodejs/aq2rdb

RUN unzip /home/nodejs/aq2rdb/aq2rdb.zip

RUN mv /home/nodejs/aq2rdb/aq2rdb-1.3.x/* /home/nodejs/aq2rdb

RUN chmod +x npmbuild && ./npmbuild

RUN mv aq2rdb-1.3.4.tar.gz /home/nodejs

WORKDIR /home/nodejs

RUN npm install aq2rdb-1.3.4.tar.gz

ENV aq_user aq_default_user

ENV aq_pass aq_default_pass

ENV ra_user ra_default_user

ENV ra_pass ra_default_pass

CMD [ "node", "/home/nodejs/node_modules/aq2rdb/aq2rdb.js", "-l", \
	"--aquariusUserName", "${aq_user}", \
	"--aquariusPassword", "${aq_pass}", \
	"--nwisRAUserName", "${ra_user}", \
	"--nwisRAPassword", "${ra_pass}"]