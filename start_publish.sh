#!/bin/bash

# This Needs To be Changed According to Where It Is Running
SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
#BASE_PYTHONPATH=${SOURCE_DIR%RabbitMQ*}RabbitMQ

#echo "BASE PYTHONPATH: $BASE_PYTHONPATH"
#
#export PYTHONPATH=$BASE_PYTHONPATH:$PYTHONPATH
#export PATH=/opt/miniconda3/bin:$PATH
#export PATH=/teamdata/markc/ProgramFiles/anaconda3/bin:/teamdata/markc/ProgramFiles/anaconda3/condabin:$PATH

# Start the publisher
# We Can Calculate The Dates And Start Publisher For Each trade From here

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

# Also This Needs To Be Changed According to Where It Is Running
#python3 $BASE_PYTHONPATH/RabbitMQ/Publisher/publisher.py $TRADE_NAME $CURR_DATE

