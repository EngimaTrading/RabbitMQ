#!/bin/bash


TRADE_NAME=$1
#Rabbit MQ Setup On Server Then Change The IP Address HOST Current LocalHost

if [ -z "$2" ]; then
    export TZ="America/Chicago"
else
    TZ="$2"
fi

HOUR=`TZ=$TZ date +"%H"`
CURR_DATE=`TZ=$TZ date '+%Y%m%d'`

DAY_OF_WEEK=`TZ=$TZ date +%u`
if [ $HOUR -ge 16 ] || [ $DAY_OF_WEEK -eq 7 ];
then
    CURR_DATE=`TZ=$TZ date -d '+1 day' '+%Y%m%d'`
fi

echo "HOUR: $HOUR | DATE: $CURR_DATE"

echo "LOG_FILE:$LOG_FILE"
while [ ! -f "$LOG_FILE" ]
do
    echo "CANNOT FIND LOG FILE $LOG_FILE"
    sleep 10
done

python3 RabbitMQ/Publisher/publisher.py $TRADE_NAME $CURR_DATE
python3 RabbitMQ/Subscriber/subscriber.py $TRADE_NAME

# Also This Needs To Be Changed According to Where It Is Running
#python3 $BASE_PYTHONPATH/RabbitMQ/Publisher/publisher.py $TRADE_NAME $CURR_DATE

