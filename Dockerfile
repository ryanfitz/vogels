FROM google/nodejs

WORKDIR /app
ADD package.json /app/
RUN npm install
ADD . /app

# CMD []
# ENTRYPOINT ["/nodejs/bin/npm", "test"]
