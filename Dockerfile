FROM ruby:3.0.1

RUN apt-get update && apt-get install -y net-tools
RUN gem install mongo
RUN gem install httparty
RUN gem install slop

ADD spaceweather.rb /home/
CMD ruby /home/spaceweather.rb -h $DBHOST -c $COLL -s $SLEEP
