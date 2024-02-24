import sys
import pandas as pd
from utils.InfluxDbManager import InfluxDBManager
from trade_plotting import PushFillsToDb, PushEngineStatesToDb, PushFactorNamesToDB, PushGitDetailsToDB, PushRMSModeToDB
import time


# Some Points To Note

# 1. I think For A Single Subscriber It May be Possible That Our Amount Of Messages Is Very high So We need To
# Consider Horizontal Scaling Or Adding Multiple Subscribers And Divide Tasks Be tween them

# 2. We Might Need To include Batch processing To Reduce The IO operations, We Are Trying To Apply Here The Batch
# processing For these Cases.

# 3. Implementing Retry For Failed Database Writes, This One is Improvement

# 4. Another Very important Thing Is Queu Durability it Should Survive Multiple Broker Restart. Important to prevent
# message loss

# 5. Another Important TradeOff To Discuss With mark And Hugo is Publisher Rate Limiting So basically There Is Some
# Work On The Subscriber Side And We Need To maintain The Sync So Limiting Might Be Needed

# 6. Other Thing I Was Thinking is Message Acknowledgement: Acknowledge the message only after it has been
# successfully written to the database. This way, if Our subscriber crashes in the middle of processing, the message
# will be requeued and processed again when the subscriber restarts.

# 7. Another Thing Is To Consider The Message Retention Policy, We Need To Consider The Retention Policy For The
# Messages In The Queue

# 8. I Am Thinking To Make These Al Common, So Like Subscriber Gets A Message It Calls The Callback Which Gives The
# Message To The Message handeler And Then The Message Handler Does The Processing And Then The Message Is Acked
# And Then The Once It Is Processed And Converted To A Form Suitable For Influx We Does The Pushing To The Influx
#  At A Common Place Maybe In Some Batches So That We Can Reduce The IO Operations. So Rethinking The Whole
class MessageHandler:
    def __init__(self, db, batch_size=1000):
        self.db = db
        self.batch_size = batch_size
        self.batch = []
        self.fills_to_db = PushFillsToDb.PushFillsToDb(db)
        self.engine_states_to_db = PushEngineStatesToDb.PushEngineStatesToDb(db)
        self.factor_names_query_to_db = PushFactorNamesToDB.PushFactorNamesQueryToDB(db)
        self.git_details_to_db = PushGitDetailsToDB.GitVersionInfoQueryToDB(db)
        self.push_rms_mode_to_db = PushRMSModeToDB.RMSModeToDB(db)

    def process_message(self, message):
        line = message.body.decode()
        self.batch.append(line)

        # if len(self.batch) >= self.batch_size:
        #     self.write_batch_to_db()

        self.git_details_to_db.ProcessLine(line)
        self.push_rms_mode_to_db.ProcessLine(line)
        self.fills_to_db.ProcessLine(line)
        self.engine_states_to_db.ProcessLine(line)
        self.factor_names_query_to_db.ProcessLine(line)
