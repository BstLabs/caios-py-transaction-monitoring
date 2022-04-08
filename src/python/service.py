# CAIOS basic service
from caios.abc.mutable_mapping import MutableMapping
from caios.abc.mapping import Mapping
from jdict import jdict
import warnings
from caios.protocol.channel import Channel
from formatting import get_types, format_process, format_transaction_data, format_ss_name, format_alert, format_email, format_alert_filename
from domain_logic import past_date, get_recent_data, refresh_data, detect_limit_violations, build_results

class TransactionMonitoring:
    """Demonstrates simple function creation using CAIOS"""
    
    def __init__(self, transactions: str, service_storage: str, alerts_storage: str, notification_mail: jdict) -> None:
        """ Service initiator
            
            :param str spreadsheet: Gspread configuration
            :param str service_storage: name of the storage for transaction data
            :param str alerts_storage: name of the storage for detected violations
            :param jdict notification_mail: sending emails configuration
            :return: None
        """
        
        self._spreadsheet = Mapping.get_mapping(transactions)
        self._transactions = MutableMapping.get_mapping(service_storage)
        self._alerts = MutableMapping.get_mapping(alerts_storage)
        self._email_channel = Channel.get_channel(notification_mail)
    
    def _prepare_data(self, window_length: int = 7) -> None:
        ''' create and store a dataframe with transactions from past days '''
        self._transactions['transactions'] = format_transaction_data(self._spreadsheet, window_length)
      
    async def _check_transactions(self, recipient: str, window_length: int) -> str:
        ''' Transaction monitoring workflow
        
            param: str recipient: email adress to which a notification will be sent
        '''
        new_sheet = self._check_new_data()
        if new_sheet == "No new data":
            return new_sheet
            
        self._update_sliding_window(new_sheet, window_length)
        
        types = self._get_types()
        violations = await gather(
            format_process(self._generated_alerts,types.single)(),
            format_process(self._generated_alerts,types.sum_recieved)(),
            format_process(self._generated_alerts,types.exchange)()
        )
        screening = self._build_results(violations)
        
        if screening.result == "No alerts":
            return "Nothing suspicious"
        
        notification = self._send_notification(screening.detected, recipient)
        return notification
        
    def _check_new_data(self) -> str:
        ''' Check if yesterday's data is available in the spreadsheet '''
        sheet = format_ss_name(0)
        if sheet not in self._spreadsheet:
            return "No new data"
        
        return sheet
            
    def _update_sliding_window(self, sheet: str, window_length: int) -> None:
        ''' Refresh file with transactions from past days '''
        old_data = self._transactions["transactions"]
        yesterday = past_date(0)
        
        if yesterday not in old_data["Date"]:
            self._transactions["transactions"] = refresh_data(
                get_recent_data(old_data,window_length),
                self._spreadsheet[sheet], 
                days_ago = 0
            )
         
    def _get_types(self) -> dict:
        ''' Returns types of violations to look for'''
        return get_types()
        
    def _generated_alerts(self, violations_type: str)-> bool:
        ''' Checking limit violations and storing alerts. Returns bool - was anything found '''
        
        limit_violations = detect_limit_violations(
            self._transactions["transactions"],
            self._spreadsheet["Rules engine"],
            violations_type
        )
        
        if limit_violations.empty:
            return False
        
        self._alerts[format_alert_filename(violations_type)] = format_alert(limit_violations)
        return True
        
    def _build_results(self, violations_flags: tuple[bool,bool,bool]) -> jdict:
        ''' Check if any violations were detected '''
        return build_results(violations_flags)
        
    def _send_notification(self, violations: jdict, recipient: str) -> str:
        ''' Sending notifications about alerts'''
        warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning) 
        email = format_email(
            self._alerts,
            violations,
            recipient
        )
        self._alerts[format_alert_filename("alerts")] = email.message
        self._email_channel.send(email)
        
        return(email.message)