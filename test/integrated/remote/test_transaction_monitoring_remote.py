from caios.test import TestCase, main, test_service
import warnings
from jdict import jdict


class TestTransaction_MonitoringRemote(TestCase):
    def test_transaction_monitoring_remote(self):
        """ Remote test """
        test_service._prepare_data()
        resp=test_service._check_transactions(
            recipient="recipient email",
            window_length=7
        )
        print(resp)
    
        
if __name__ == "__main__":
    main()
