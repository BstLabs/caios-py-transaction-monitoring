from caios.test import TestCase, main, project_name
from caios.abc.mapping import Mapping
import os
from service import TransactionMonitoring
from jdict import jdict
import json
import os
from caios.protocol.channel import Channel

project_name = os.path.basename(__file__.rpartition('/test/')[0]) 
        
class TestTransaction_MonitoringLocal(TestCase):
    def test_transaction_monitoring_local(self):
        """ Local system test """
        srv = TransactionMonitoring(
            json.dumps(jdict(
                    resource='GSpread',
                    interface='Mapping',
                    secret='path to secret',
                    scope=['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'],
                    workbook='TransactionMonitoring',
                    name = 'transactions'
                )
            ), 
            f'{os.environ["CAIOS_USER_STORAGE"]}/{project_name}/transactions', 
            f'{os.environ["CAIOS_USER_STORAGE"]}/{project_name}/alerts', 
            jdict(
                resource="SES",
                interface=Channel,
                name="notification_mail",
                sender="sender email"
            )
        )
        
        params = jdict(recipient="recipient email")  
        
        srv._prepare_data(7)
        new_sheet = srv._check_new_data()
        if new_sheet == "No new data":
            print(new_sheet)
            return new_sheet
        srv._update_sliding_window(new_sheet,7)
        
        single_transfer=srv._generated_alerts("single")
        sum_recieved=srv._generated_alerts("sum_recieved")
        exchanged_amount=srv._generated_alerts("exchange")
        
        screening = srv._build_results([single_transfer,sum_recieved,exchanged_amount])
        
        if screening.result == "Not found":
            print("Nothing suspicious today")
            return("Nothing suspicious today")
        
        notification = srv._send_notification(screening.detected, params.recipient)
        print(notification)
        return notification

if __name__ == "__main__":
    main()